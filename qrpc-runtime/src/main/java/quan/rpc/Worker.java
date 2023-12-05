package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.Protocol.Request;
import quan.rpc.Protocol.Response;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 单线程工作者
 *
 * @author quanchangnai
 */
public class Worker implements Executor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final ThreadLocal<Worker> threadLocal = new ThreadLocal<>();

    private static int nextId = 1;

    private final int id = nextId++;

    private final Node node;

    private ExecutorService executor;

    private Thread thread;

    //暂存还未启动时提交的需要执行的任务
    private final Queue<Runnable> tempTasks = new ConcurrentLinkedQueue<>();

    //管理的所有服务，key:服务ID
    private final Map<Object, Service<?>> services = newMap();

    private final Map<Long, Promise<Object>> mappedPromises = newMap();

    private final SortedSet<Promise<Object>> sortedPromises = new TreeSet<>();

    private final SortedSet<DelayedResult<Object>> delayedResults = new TreeSet<>();

    private final TimerQueue timerQueue = new TimerQueue(this);

    private int nextCallId = 1;

    private volatile long updateReadyTime;

    private volatile long updateStartTime;

    private long printUpdateIntervalTime;

    private long printUpdateWaitTime;

    private long printUpdateCostTime;

    protected Worker(Node node) {
        this.node = node;
    }

    protected <K, V> Map<K, V> newMap() {
        return new HashMap<>();
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

    public boolean isRunning() {
        return executor != null && !executor.isShutdown();
    }

    public void addService(Service<?> service) {
        node.addService(service, this);
    }

    protected void doAddService(Service<?> service) {
        service.setWorker(this);
        services.put(service.getId(), service);
        if (isRunning()) {
            initService(service);
        }
    }

    public void removeService(Object serviceId) {
        if (!services.containsKey(serviceId)) {
            logger.error("服务[{}]不存在", serviceId);
        } else {
            node.removeService(serviceId);
        }
    }

    protected void doRemoveService(Service<?> service) {
        Object serviceId = service.getId();
        if (isRunning()) {
            destroyService(service);
        }

        service.setWorker(null);
        services.remove(serviceId);
    }

    protected void initService(Service<?> service) {
        if (service.state == 0) {
            try {
                service.state = 1;
                service.init();
            } catch (Exception e) {
                logger.error("服务[{}]初始化异常", service.getId(), e);
            }
        }
    }

    protected void destroyService(Service<?> service) {
        if (service.state == 1) {
            try {
                service.state = 2;
                service.destroy();
            } catch (Exception e) {
                logger.error("服务[{}]销毁异常", service.getId(), e);
            }
        }
    }

    public Service<?> getService(Object serviceId) {
        return services.get(serviceId);
    }

    public Collection<Service<?>> getServices() {
        return Collections.unmodifiableCollection(services.values());
    }

    protected void start() {
        executor = newExecutor();
        tempTasks.forEach(executor::execute);
        tempTasks.clear();
        execute(() -> {
            for (Service<?> service : services.values()) {
                initService(service);
            }
        });
    }

    protected void stop() {
        execute(() -> {
            for (Service<?> service : services.values()) {
                destroyService(service);
            }
            executor.shutdown();
        });
    }

    protected ExecutorService newExecutor() {
        return Executors.newFixedThreadPool(1, this::newThread);
    }

    protected Thread newThread(Runnable task) {
        thread = new Thread(() -> {
            threadLocal.set(this);
            task.run();
        }, "worker-" + id);

        return thread;
    }

    protected void addSortedPromise(Promise<Object> promise) {
        sortedPromises.add(promise);
    }

    protected void removeSortedPromise(Promise<Object> promise) {
        sortedPromises.remove(promise);
    }

    protected void addDelayedResult(DelayedResult<Object> delayedResult) {
        delayedResults.add(delayedResult);
    }

    protected boolean containsDelayedResult(DelayedResult<Object> delayedResult) {
        return delayedResults.contains(delayedResult);
    }


    @Override
    @SuppressWarnings("NullableProblems")
    public void execute(Runnable task) {
        if (executor == null) {
            tempTasks.add(task);
        } else {
            executor.execute(task);
        }
    }

    /**
     * 当前时间戳，可能会在系统时间的基础上加偏移
     */
    public long getTime() {
        return node.getTime();
    }

    /**
     * 创建一个延迟执行的定时器
     *
     * @see TimerQueue#newTimer(Runnable, long)
     */
    public Timer newTimer(Runnable task, long delay) {
        return timerQueue.newTimer(task, delay);
    }

    /**
     * 创建一个周期性执行的定时器
     *
     * @see TimerQueue#newTimer(Runnable, long)
     */
    public Timer newTimer(Runnable task, long delay, long period) {
        return timerQueue.newTimer(task, delay, period);
    }

    /**
     * 创建一个基于cron表达式的定时器
     *
     * @see TimerQueue#newTimer(Runnable, String)
     */
    public Timer newTimer(Runnable task, String cron) {
        return timerQueue.newTimer(task, cron);
    }


    /**
     * 发起刷帧，上一次刷帧还没有结束不执行新的刷帧
     */
    protected void update() {
        if (updateReadyTime <= 0) {
            updateReadyTime = System.currentTimeMillis();
            execute(this::doUpdate);
        }

        checkUpdateIntervalTime();
    }

    protected void checkUpdateIntervalTime() {
        if (updateStartTime <= 0) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        long updateIntervalTime = currentTime - updateStartTime;

        if (updateIntervalTime > getNode().getConfig().getMaxUpdateInterval() && currentTime - printUpdateIntervalTime > 5000) {
            printUpdateIntervalTime = currentTime;
            if (thread != null) {
                StringBuilder sb = new StringBuilder();
                for (StackTraceElement traceElement : thread.getStackTrace()) {
                    sb.append("\tat ").append(traceElement).append("\n");
                }
                logger.error("线程工作者[{}]帧率过低，距离上次刷帧已经过了{}ms，线程[{}]可能执行了耗时任务\n{}", id, updateIntervalTime, thread, sb);
            } else {
                logger.error("线程工作者[{}]帧率过低，距离上次刷帧已经过了{}ms，可能执行了耗时任务", id, updateIntervalTime);
            }
        }
    }

    /**
     * 执行刷帧
     */
    private void doUpdate() {
        try {
            updateStartTime = System.currentTimeMillis();
            updateTimerQueue();
            for (Service<?> service : services.values()) {
                updateService(service);
            }
            expirePromises();
            expireDelayedResults();
            checkUpdateTime();
        } finally {
            updateReadyTime = 0;
        }
    }

    protected void updateTimerQueue() {
        timerQueue.update();
    }

    protected void updateService(Service<?> service) {
        try {
            service.updateTimerQueue();
            service.update();
        } catch (Exception e) {
            logger.error("服务[{}]刷帧出错", service.getId(), e);
        }
    }

    protected void expirePromises() {
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
            expirePromise(promise);
        }
    }

    protected void expirePromise(Promise<Object> promise) {
        promise.onExpired();
    }

    protected void expireDelayedResults() {
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
            expirePromise(delayedResult);
        }
    }

    private void checkUpdateTime() {
        long currentTime = System.currentTimeMillis();

        long updateWaitTime = updateStartTime - updateReadyTime;
        if (updateWaitTime > node.getConfig().getMaxUpdateWaitTime() && currentTime - printUpdateWaitTime > 5000) {
            printUpdateWaitTime = currentTime;
            logger.error("线程工作者[{}]的刷帧等待时间({}ms)过长", id, updateWaitTime);
        }

        long updateCostTime = System.currentTimeMillis() - updateStartTime;
        if (updateCostTime > node.getConfig().getMaxUpdateCostTime() && currentTime - printUpdateCostTime > 5000) {
            printUpdateCostTime = currentTime;
            logger.error("线程工作者[{}]的刷帧消耗时间({}ms)过长", id, updateCostTime);
        }
    }

    protected int getCallId() {
        int callId = nextCallId++;
        if (nextCallId < 0) {
            nextCallId = 1;
        }
        return callId;
    }

    /**
     * 发送RPC请求
     *
     * @param proxy            服务代理
     * @param methodId         要调用的方法ID
     * @param signature        方法签名字符串
     * @param securityModifier 方法安全部修饰符
     * @param params           方法参数列表
     * @param <R>              方法的返回结果泛型
     */
    @SuppressWarnings("unchecked")
    protected <R> Promise<R> sendRequest(Proxy proxy, int methodId, String signature, int securityModifier, Object... params) {
        long callId = (long) this.id << 32 | getCallId();

        Promise<Object> promise = new Promise<>(callId, signature, this);
        mappedPromises.put(promise.getCallId(), promise);
        addSortedPromise(promise);

        sendRequest(proxy, promise, methodId, securityModifier, params);

        return (Promise<R>) promise;
    }

    private void sendRequest(Proxy proxy, Promise<Object> promise, int methodId, int securityModifier, Object... params) {
        int targetNodeId;
        Object serviceId;

        try {
            targetNodeId = proxy._getNodeId$(this);
            serviceId = proxy._getServiceId$(this);
        } catch (Exception e) {
            handleSendRequestError(promise, e);
            return;
        }

        if (promise.isExpired()) {
            logger.error("发送RPC请求，已过期无需发送，targetNodeId:{}，serviceId:{}", targetNodeId, serviceId);
            return;
        }

        if (targetNodeId == -1) {
            //延迟重新发送
            newTimer(() -> sendRequest(proxy, promise, methodId, securityModifier, params), getNode().getConfig().getUpdateInterval());
            return;
        }

        try {
            promise.setExpiredTime();
            makeParamsSafe(targetNodeId, securityModifier, params);
            Request request = new Request(node.getId(), promise.getCallId(), serviceId, methodId, params);
            node.sendRequest(targetNodeId, request, securityModifier);
        } catch (Exception e) {
            handleSendRequestError(promise, e);
        }
    }

    private void handleSendRequestError(Promise<Object> promise, Exception e) {
        mappedPromises.remove(promise.getCallId());
        removeSortedPromise(promise);
        execute(() -> promise.setException(e));
    }

    protected Object cloneObject(Object object) {
        return SerializeUtils.clone(object, true);
    }

    /**
     * 如果有参数是不安全的,则需要复制它以保证安全
     *
     * @param securityModifier 1:标记所有参数都是安全的，参考 {@link Endpoint#safeArgs()}
     */
    private void makeParamsSafe(int targetNodeId, int securityModifier, Object[] params) {
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
     * @param securityModifier 2:标记返回结果是安全的，参考 {@link Endpoint#safeReturn()}
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

        Service<?> service = services.get(serviceId);
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
                delayedResult.setHandler();
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
        //noinspection unchecked
        if (!containsDelayedResult(delayedResult)) {
            return;
        }

        int originNodeId = delayedResult.getOriginNodeId();
        Object result = makeResultSafe(originNodeId, delayedResult.getSecurityModifier(), delayedResult.getResult());
        Response response = new Response(node.getId(), delayedResult.getCallId(), result, delayedResult.getExceptionStr());
        node.sendResponse(originNodeId, response);
    }

    protected void handleResponse(Response response) {
        long callId = response.getCallId();
        if (!mappedPromises.containsKey(callId)) {
            logger.error("处理RPC响应，调用[{}]不存在", callId);
        } else {
            handlePromise(callId, CallException.create(response.getException()), response.getResult());
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

        removeSortedPromise(promise);

        if (exception != null) {
            promise.setException(exception);
        } else {
            promise.setResult(result);
        }
    }

    public <R> DelayedResult<R> newDelayedResult() {
        DelayedResult<R> delayedResult = new DelayedResult<>(this);
        //noinspection unchecked
        addDelayedResult((DelayedResult<Object>) delayedResult);
        return delayedResult;
    }

}
