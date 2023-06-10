package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.message.CodedBuffer;
import quan.message.DefaultCodedBuffer;
import quan.rpc.protocol.Request;
import quan.rpc.protocol.Response;
import quan.rpc.serialize.ObjectReader;
import quan.rpc.serialize.ObjectWriter;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 工作线程
 *
 * @author quanchangnai
 */
public class Worker implements Executor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final ThreadLocal<Worker> threadLocal = new ThreadLocal<>();

    private static int nextId = 1;

    private final int id = nextId++;

    private volatile boolean running;

    private final Node node;

    private Thread thread;

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    //刷帧发起时间
    private volatile long updateLaunchTime;

    //刷帧开始时间
    private volatile long updateStartTime;

    private long stackTraceTime;

    //管理的所有服务，key:服务ID
    private final Map<Object, Service> allServices = new HashMap<>();

    private final Set<UpdatableService> updatableServices = new HashSet<>();

    private int nextCallId = 1;

    private final Map<Long, Promise<Object>> mappedPromises = new HashMap<>();

    //按时间排序
    private final TreeSet<Promise<Object>> sortedPromises = new TreeSet<>(Comparator.comparingLong(Promise::getExpiredTime));

    private final TreeSet<DelayedResult<Object>> delayedResults = new TreeSet<>(Comparator.comparingLong(DelayedResult::getExpiredTime));

    //定时任务
    private final PriorityQueue<TimerTask> timerTasks = new PriorityQueue<>(Comparator.comparingLong(TimerTask::getRunTime));

    private ObjectWriter writer;

    private ObjectReader reader;

    protected Worker(Node node) {
        this.node = node;
    }

    public static Worker current() {
        return threadLocal.get();
    }

    public int getId() {
        return id;
    }

    public Node getNode() {
        return node;
    }

    public void addService(Service service) {
        node.addService(this, service);
    }

    protected void doAddService(Service service) {
        service.worker = this;
        allServices.put(service.getId(), service);
        if (service instanceof UpdatableService) {
            updatableServices.add((UpdatableService) service);
        }
        if (running) {
            initService(service);
        }
    }

    private void initService(Service service) {
        try {
            service.init();
        } catch (Exception e) {
            logger.error("服务[{}]初始化异常", service.getId(), e);
        }
    }

    public void removeService(Object serviceId) {
        if (!allServices.containsKey(serviceId)) {
            logger.error("服务[{}]不存在", serviceId);
        } else {
            node.removeService(serviceId);
        }
    }

    protected void doRemoveService(Service service) {
        Object serviceId = service.getId();
        if (running) {
            destroyService(service);
        }
        service.worker = null;
        allServices.remove(serviceId);
        if (service instanceof UpdatableService) {
            updatableServices.remove(service);
        }

    }

    private void destroyService(Service service) {
        try {
            service.destroy();
        } catch (Exception e) {
            logger.error("服务[{}]销毁异常", service.getId(), e);
        }
    }

    protected void start() {
        thread = new Thread(this::run, "worker-" + id);
        thread.start();

        execute(() -> allServices.values().forEach(this::initService));
    }

    protected void stop() {
        execute(() -> {
            allServices.values().forEach(this::destroyService);
            running = false;
        });
    }

    protected void run() {
        threadLocal.set(this);
        running = true;

        while (running) {
            try {
                taskQueue.take().run();
            } catch (Throwable e) {
                logger.error("", e);
            }
        }

        taskQueue.clear();
        threadLocal.set(null);
        thread = null;
    }

    /**
     * 执行任务
     */
    @Override
    @SuppressWarnings("NullableProblems")
    public void execute(Runnable task) {
        Objects.requireNonNull(task, "参数[task]不能为空");
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            logger.error("", e);
        }
    }

    /**
     * 在下一帧执行任务
     */
    public void schedule(Runnable task) {
        Objects.requireNonNull(task, "参数[task]不能为空");
        if (Thread.currentThread() == this.thread) {
            addTimerTask(task, 0, 0);
        } else {
            execute(() -> addTimerTask(task, 0, 0));
        }
    }

    /**
     * 延迟执行任务
     *
     * @see #schedule(Runnable, long, long)
     */
    public void schedule(Runnable task, long delay) {
        schedule(task, delay, 0);
    }

    /**
     * 周期性执行任务
     *
     * @param task   任务
     * @param delay  延迟时间
     * @param period 周期间隔时间
     */
    public void schedule(Runnable task, long delay, long period) {
        Objects.requireNonNull(task, "参数[task]不能为空");

        int updateInterval = node.getUpdateInterval();
        if (delay < updateInterval) {
            throw new IllegalArgumentException("参数[delay]不能小于" + updateInterval);
        }
        if (period > 0 && period < updateInterval) {
            throw new IllegalArgumentException("参数[period]不能小于" + updateInterval);
        }

        if (Thread.currentThread() == this.thread) {
            addTimerTask(task, delay, period);
        } else {
            execute(() -> addTimerTask(task, delay, period));
        }
    }

    private void addTimerTask(Runnable task, long delay, long period) {
        TimerTask timerTask = new TimerTask();
        timerTask.runTime = System.currentTimeMillis() + Math.max(delay, 0);
        timerTask.period = Math.max(period, 0);
        timerTask.task = task;
        timerTasks.offer(timerTask);
    }

    /**
     * 发起刷帧，上一次刷帧还没有结束不执行新的刷帧
     */
    protected void update() {
        long currentTime = System.currentTimeMillis();

        if (updateLaunchTime <= 0) {
            updateLaunchTime = currentTime;
            execute(this::doUpdate);
        }

        long intervalTime = currentTime - updateStartTime;
        if (updateStartTime > 0 && intervalTime > getNode().getUpdateInterval() * 2L && currentTime - stackTraceTime > 10000) {
            stackTraceTime = currentTime;
            StringBuilder stackTrace = new StringBuilder();
            for (StackTraceElement traceElement : thread.getStackTrace()) {
                stackTrace.append("\tat ").append(traceElement).append("\n");
            }
            logger.error("工作线程[{}]帧率过低，距离上次刷帧已经过了{}ms，线程[{}]可能执行了耗时任务\n{}", id, intervalTime, thread, stackTrace);
        }
    }

    /**
     * 执行刷帧
     */
    private void doUpdate() {
        try {
            updateStartTime = System.currentTimeMillis();
            updateServices();
            runTimerTasks();
            expirePromises();
            expireDelayedResults();
            checkUpdateTime();
        } finally {
            updateLaunchTime = 0;
        }
    }

    private void updateServices() {
        for (UpdatableService service : updatableServices) {
            try {
                service.update();
            } catch (Throwable e) {
                logger.error("服务[{}]刷帧出错", service.getId(), e);
            }
        }
    }

    private void runTimerTasks() {
        TimerTask timerTask = timerTasks.peek();
        List<TimerTask> periodicTasks = new ArrayList<>();

        while (timerTask != null && timerTask.isTimeUp()) {
            timerTasks.poll();
            try {
                timerTask.run();
            } catch (Exception e) {
                logger.error("", e);
            }

            if (timerTask.period > 0) {
                periodicTasks.add(timerTask);
            }
            timerTask = timerTasks.peek();
        }

        timerTasks.addAll(periodicTasks);
    }

    private void expirePromises() {
        if (sortedPromises.isEmpty()) {
            return;
        }

        Iterator<Promise<Object>> iterator = sortedPromises.iterator();
        while (iterator.hasNext()) {
            Promise<Object> promise = iterator.next();
            if (!promise.isExpired()) {
                return;
            }
            iterator.remove();
            mappedPromises.remove(promise.getCallId());
            promise.setTimeout();
        }
    }

    private void expireDelayedResults() {
        if (delayedResults.isEmpty()) {
            return;
        }

        Iterator<DelayedResult<Object>> iterator = delayedResults.iterator();
        while (iterator.hasNext()) {
            DelayedResult<Object> delayedResult = iterator.next();
            if (!delayedResult.isExpired()) {
                return;
            }
            iterator.remove();
            delayedResults.remove(delayedResult);
            delayedResult.setTimeout();
        }
    }

    private void checkUpdateTime() {
        long updateWaitTime = updateStartTime - updateLaunchTime;
        if (updateWaitTime > 10) {
            logger.error("工作线程[{}]的刷帧等待时间({}ms)过长", id, updateWaitTime);
        } else if (updateWaitTime > 2) {
            logger.warn("工作线程[{}]的刷帧等待时间({}ms)偏长", id, updateWaitTime);
        }

        long updateCostTime = System.currentTimeMillis() - updateStartTime;
        if (updateCostTime > node.getUpdateInterval()) {
            logger.warn("工作线程[{}]的刷帧消耗时间({}ms)过长", id, updateCostTime);
        } else if (updateCostTime * 2 >= node.getUpdateInterval()) {
            logger.error("工作线程[{}]的刷帧消耗时间({}ms)偏长", id, updateCostTime);
        }
    }

    /**
     * @see NodeIdResolver
     */
    private int resolveTargetNodeId(Proxy proxy) {
        int targetNodeId = proxy._getNodeId$();
        if (targetNodeId >= 0) {
            return targetNodeId;
        }

        NodeIdResolver proxyNodeIdResolver = proxy._getNodeIdResolver$();
        if (proxyNodeIdResolver != null) {
            return proxyNodeIdResolver.resolve(proxy);
        }

        NodeIdResolver globalNodeIdResolver = node.getTargetNodeIdResolver();
        if (globalNodeIdResolver != null) {
            return globalNodeIdResolver.resolve(proxy);
        }

        return 0;
    }

    @SuppressWarnings("unchecked")
    protected <R> Promise<R> sendRequest(Proxy proxy, String signature, int securityModifier, int methodId, Object... params) {
        long callId = (long) this.id << 32 | nextCallId++;
        if (nextCallId < 0) {
            nextCallId = 1;
        }

        Promise<Object> promise = new Promise<>(callId, signature, this);
        mappedPromises.put(promise.getCallId(), promise);
        sortedPromises.add(promise);

        sendRequest(proxy, promise, securityModifier, methodId, params);

        return (Promise<R>) promise;
    }

    private void sendRequest(Proxy proxy, Promise<Object> promise, int securityModifier, int methodId, Object... params) {
        int targetNodeId;
        try {
            targetNodeId = resolveTargetNodeId(proxy);
        } catch (Exception e) {
            afterSendRequestError(promise, e);
            return;
        }

        if (promise.isExpired()) {
            logger.error("发送RPC请求，已过期无需发送，targetNodeId:{}，serviceId:{}", targetNodeId, proxy._getServiceId());
            return;
        }

        if (targetNodeId < 0) {
            schedule(() -> sendRequest(proxy, promise, securityModifier, methodId, params));
            return;
        }

        try {
            makeParamSafe(targetNodeId, securityModifier, params);
            Request request = new Request(node.getId(), promise.getCallId(), proxy._getServiceId(), methodId, params);
            node.sendRequest(targetNodeId, request, securityModifier);
        } catch (Exception e) {
            afterSendRequestError(promise, e);
        }
    }

    private void afterSendRequestError(Promise<Object> promise, Exception e) {
        mappedPromises.remove(promise.getCallId());
        sortedPromises.remove(promise);
        execute(() -> promise.setException(e));
    }

    private Object cloneObject(Object object) {
        if (writer == null) {
            CodedBuffer buffer = new DefaultCodedBuffer();
            writer = node.getWriterFactory().apply(buffer);
            reader = node.getReaderFactory().apply(buffer);
        } else {
            writer.getBuffer().clear();
        }
        writer.write(object);
        return reader.read();
    }

    /**
     * 如果有参数是不安全的,则需要复制它以保证安全
     *
     * @param securityModifier 1:标记所有参数都是安全的，参考 {@link Endpoint#paramSafe()}
     */
    private void makeParamSafe(int targetNodeId, int securityModifier, Object[] params) {
        if (targetNodeId != 0 && targetNodeId != this.node.getId()) {
            return;
        }

        if (params == null || (securityModifier & 0b01) == 0b01) {
            return;
        }

        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (!ConstantUtils.isConstant(param)) {
                params[i] = cloneObject(param);
            }
        }
    }

    /**
     * 如果返回结果是不安全的，则需要复制它以保证安全
     *
     * @param securityModifier 2:标记返回结果是安全的，参考 {@link Endpoint#resultSafe()}
     */
    private Object makeResultSafe(int originNodeId, int securityModifier, Object result) {
        if (originNodeId != this.node.getId()) {
            return result;
        }

        if (ConstantUtils.isConstant(result) || (securityModifier & 0b10) == 0b10) {
            return result;
        } else {
            return cloneObject(result);
        }
    }

    protected void handleRequest(Request request, int securityModifier) {
        int originNodeId = request.getOriginNodeId();
        long callId = request.getCallId();
        Object serviceId = request.getServiceId();
        Object result = null;
        String exception = null;

        Service service = allServices.get(serviceId);
        if (service == null) {
            logger.error("处理RPC请求，服务[{}]不存在，originNodeId:{}，callId:{}", serviceId, originNodeId, callId);
            return;
        }

        try {
            result = service.call(request.getMethodId(), request.getParams());
        } catch (Throwable e) {
            exception = e.toString();
            logger.error("处理RPC请求，方法执行异常，originNodeId:{}，callId:{}", originNodeId, callId, e);
        }

        if (result instanceof DelayedResult) {
            DelayedResult<?> delayedResult = (DelayedResult<?>) result;
            if (!delayedResult.isFinished()) {
                delayedResult.setCallId(callId);
                delayedResult.setOriginNodeId(originNodeId);
                delayedResult.setSecurityModifier(securityModifier);
                return;
            } else {
                exception = delayedResult.getExceptionStr();
                if (exception == null) {
                    result = makeResultSafe(originNodeId, securityModifier, delayedResult.getResult());
                }
            }
        }

        Response response = new Response(node.getId(), callId, result, exception);
        node.sendResponse(originNodeId, response);
    }

    @SuppressWarnings("rawtypes")
    protected void handleDelayedResult(DelayedResult delayedResult) {
        int originNodeId = delayedResult.getOriginNodeId();
        Object result = makeResultSafe(originNodeId, delayedResult.getSecurityModifier(), delayedResult.getResult());
        Response response = new Response(node.getId(), delayedResult.getCallId(), result, delayedResult.getExceptionStr());
        node.sendResponse(originNodeId, response);
    }

    protected void handleResponse(Response response) {
        long callId = response.getCallId();
        if (!mappedPromises.containsKey(callId)) {
            logger.error("处理RPC响应，调用[{}]不存在或者已超时", callId);
        } else {
            handlePromise(callId, CallException.create(response), response.getResult());
        }
    }

    protected void handlePromise(long callId, Exception exception, Object result) {
        Promise<Object> promise = mappedPromises.remove(callId);
        if (promise == null) {
            if (exception != null) {
                logger.error("调用[{}]方法出错", callId, exception);
            }
            return;
        }

        sortedPromises.remove(promise);

        if (exception != null) {
            promise.setException(exception);
        } else {
            promise.setResult(result);
        }
    }

    public <R> DelayedResult<R> newDelayedResult() {
        return new DelayedResult<>(this);
    }

    /**
     * 定时任务
     */
    private static class TimerTask implements Runnable {

        long runTime;

        //大于0代表该任务是周期任务
        long period;

        Runnable task;

        long getRunTime() {
            return runTime;
        }

        boolean isTimeUp() {
            return runTime < System.currentTimeMillis();
        }

        @Override
        public void run() {
            try {
                task.run();
            } finally {
                if (period > 0) {
                    runTime = System.currentTimeMillis() + period;
                }
            }
        }

    }

}
