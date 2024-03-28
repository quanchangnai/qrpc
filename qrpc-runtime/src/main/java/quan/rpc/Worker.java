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

    private final Map<Long, Promise<?>> mappedPromises = newMap();

    private final SortedSet<Promise<?>> sortedPromises = newSortedSet();


    public final TimerMgr timerMgr = new TimerMgr(this);

    private int nextCallId = 1;

    private volatile long updateReadyTime;

    private volatile long updateStartTime;

    private long printUpdateIntervalTime;

    private long printUpdateWaitTime;

    private long printUpdateCostTime;

    protected Worker(Node node) {
        this.node = Objects.requireNonNull(node);
    }

    protected <K, V> Map<K, V> newMap() {
        return new HashMap<>();
    }

    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    protected <E> SortedSet<E> newSortedSet() {
        return new TreeSet<>();
    }

    public static Worker current() {
        return threadLocal.get();
    }

    public int getId() {
        return id;
    }

    public Object getFlag() {
        return id;
    }

    public Node getNode() {
        return node;
    }

    public boolean isRunning() {
        return executor != null && !executor.isShutdown();
    }

    public boolean isSingleThread() {
        return thread != null;
    }

    public boolean isThreadPool() {
        return this instanceof ThreadPoolWorker;
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
                service.initTimerMgr();
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

    @Override
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
     * @see #getTime()
     */
    public static long currentTime() {
        return Worker.current().getTime();
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
                logger.error("线程工作者[{}]帧率过低,距离上次刷帧已经过了{}ms,线程[{}]可能执行了耗时任务\n{}", id, updateIntervalTime, thread, sb);
            } else {
                logger.error("线程工作者[{}]帧率过低,距离上次刷帧已经过了{}ms,可能执行了耗时任务", id, updateIntervalTime);
            }
        }
    }

    /**
     * 执行刷帧
     */
    private void doUpdate() {
        try {
            updateStartTime = System.currentTimeMillis();
            timerMgr.update();
            for (Service<?> service : services.values()) {
                service.getTimerMgr().update();
            }
            expirePromises();
            checkUpdateTime();
        } finally {
            updateReadyTime = 0;
        }
    }

    protected void expirePromises() {
        if (sortedPromises.isEmpty()) {
            return;
        }

        Iterator<Promise<?>> iterator = sortedPromises.iterator();
        while (iterator.hasNext()) {
            Promise<?> promise = iterator.next();
            if (!promise.isExpired()) {
                return;
            }

            iterator.remove();
            if (promise.getCallId() > 0) {
                mappedPromises.remove(promise.getCallId());
            }

            promise.expire();
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
     * @param <R>            方法的返回结果泛型
     * @param proxy          服务代理
     * @param methodId       方法ID
     * @param methodLabel    方法标签
     * @param methodSecurity 方法的安全修饰符
     * @param expiredTime    方法的过期时间
     * @param params         方法参数列表
     */
    @SuppressWarnings("unchecked")
    protected <R> Promise<R> sendRequest(Proxy proxy, int methodId, String methodLabel, int methodSecurity, int expiredTime, Object... params) {
        long callId = (long) this.id << 32 | getCallId();

        Promise<Object> promise = new Promise<>(this);
        promise.setCallId(callId);
        promise.setMethod(methodLabel);
        promise.calcExpiredTime(expiredTime);

        mappedPromises.put(callId, promise);
        sortedPromises.add(promise);

        sendRequest(proxy, promise, methodId, methodSecurity, params);

        return (Promise<R>) promise;
    }

    private void sendRequest(Proxy proxy, Promise<Object> promise, int methodId, int methodSecurity, Object... params) {
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
            logger.error("发送RPC请求,已过期无需发送,targetNodeId:{},serviceId:{}", targetNodeId, serviceId);
            return;
        }

        if (targetNodeId == -1) {
            //延迟重新发送
            timerMgr.newTimer(() -> sendRequest(proxy, promise, methodId, methodSecurity, params), getNode().getConfig().getUpdateInterval());
            return;
        }

        try {
            makeSafeParams(targetNodeId, methodSecurity, params);
            Request request = new Request(node.getId(), promise.getCallId(), serviceId, methodId, params);
            node.sendRequest(targetNodeId, request, methodSecurity);
        } catch (Exception e) {
            handleSendRequestError(promise, e);
        }
    }

    private void handleSendRequestError(Promise<Object> promise, Exception e) {
        mappedPromises.remove(promise.getCallId());
        sortedPromises.remove(promise);
        execute(() -> promise.setException(e));
    }

    protected Object cloneObject(Object object) {
        return SerializeUtils.clone(object, true);
    }

    /**
     * 如果方法有参数是不安全的,则需要复制它以保证安全
     *
     * @param methodSecurity 1:参考 {@link Endpoint#safeArgs()}
     */
    private void makeSafeParams(int targetNodeId, int methodSecurity, Object[] params) {
        if (params == null || targetNodeId != 0 && targetNodeId != this.node.getId() || (methodSecurity & 0b01) == 0b01) {
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
     * 如果方法的返回结果是不安全的，则需要复制它以保证安全
     *
     * @param security 2:参考 {@link Endpoint#safeReturn()}
     */
    private Object makeSafeResult(int originNodeId, int security, Object result) {
        if (result == null || originNodeId != this.node.getId()) {
            return result;
        }

        if (ConstantUtils.isConstant(result) || (security & 0b10) == 0b10) {
            return result;
        } else {
            return cloneObject(result);
        }
    }

    protected void handleRequest(Request request, int security) {
        int originNodeId = request.getOriginNodeId();
        long callId = request.getCallId();
        Object serviceId = request.getServiceId();
        int methodId = request.getMethodId();
        Service<?> service = services.get(serviceId);

        if (service == null) {
            logger.error("处理RPC请求,服务[{}]不存在,callId:{},originNodeId:{}", serviceId, callId, originNodeId);
            node.sendResponse(originNodeId, callId, null, String.format("服务[%s]不存在", serviceId));
            return;
        }

        Invoker invoker;
        Object result;

        try {
            invoker = service.getInvoker();
            result = invoker.invoke(service, methodId, request.getParams());
        } catch (Throwable e) {
            logger.error("处理RPC请求,方法执行异常,callId:{},originNodeId:{}", callId, originNodeId, e);
            Object exception = toResponseException(originNodeId, e);
            node.sendResponse(originNodeId, callId, null, exception);
            return;
        }

        if (!(result instanceof Promise)) {
            node.sendResponse(originNodeId, callId, result, null);
            return;
        }

        Promise<?> promise = (Promise<?>) result;

        if (promise instanceof DelayedResult) {
            if (promise.getMethod() == null) {
                promise.setMethod(invoker.getMethodLabel(methodId));
                int expiredTime = invoker.getExpiredTime(methodId);
                if (expiredTime > 0) {
                    promise.calcExpiredTime(expiredTime);
                    //更新基于过期时间的排序
                    sortedPromises.remove(promise);
                    sortedPromises.add(promise);
                }
            } else {
                logger.error("方法不能返回已被使用过的{},method:", promise.getClass().getSimpleName(), invoker.getMethodLabel(methodId));
            }
        }

        promise._completely(() -> {
            if (promise instanceof DelayedResult && !sortedPromises.remove(promise)) {
                return;
            }

            Object realResult = makeSafeResult(originNodeId, security, promise.getResult());
            Object exception = toResponseException(originNodeId, promise);
            node.sendResponse(originNodeId, callId, realResult, exception);
        });
    }

    public <R> DelayedResult<R> newDelayedResult() {
        DelayedResult<R> delayedResult = new DelayedResult<>(this);
        sortedPromises.add(delayedResult);
        return delayedResult;
    }

    private Object toResponseException(int targetNodeId, Throwable e) {
        if (targetNodeId != this.node.getId() && this.node.getConfig().isThrowExceptionToRemote()) {
            return e;
        } else {
            return e.toString();
        }
    }

    private Object toResponseException(int targetNodeId, Promise<?> promise) {
        if (!promise.isFailed()) {
            return null;
        } else if (targetNodeId != this.node.getId() && this.node.getConfig().isThrowExceptionToRemote()) {
            return promise.getException();
        } else {
            return promise.getExceptionStr();
        }
    }

    protected void handleResponse(Response response) {
        long callId = response.getCallId();
        if (!mappedPromises.containsKey(callId)) {
            logger.error("处理RPC响应,调用[{}]不存在,originNodeId:{}", callId, response.getOriginNodeId());
        } else {
            Object exception = response.getException();
            if (exception == null) {
                handleResponse(callId, response.getResult(), null);
            } else if (exception instanceof Throwable) {
                handleResponse(callId, null, new CallException((Throwable) exception));
            } else {
                handleResponse(callId, null, new CallException(exception.toString()));
            }
        }
    }

    protected void handleResponse(long callId, Object result, Exception exception) {
        @SuppressWarnings("unchecked")
        Promise<Object> promise = (Promise<Object>) mappedPromises.remove(callId);

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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "id=" + id +
                '}';
    }

}
