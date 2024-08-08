package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.Protocol.Request;
import quan.rpc.Protocol.Response;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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

    protected final Set<TimerMgr> timerMgrSet = newSet();

    private final TimerMgr timerMgr = new TimerMgr(this);

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

    protected <E> Set<E> newSet() {
        return new HashSet<>();
    }

    protected <E extends Comparable<E>> SortedSet<E> newSortedSet() {
        return new TreeSet<>();
    }

    public static Worker current() {
        return threadLocal.get();
    }

    public int getId() {
        return id;
    }

    public String getFlag() {
        return node.getConfig().getSingleThreadWorkerFlag() + id;
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
        service.init$();
    }

    protected void destroyService(Service<?> service) {
        service.destroy$();
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
        int maxUpdateInterval = getNode().getConfig().getMaxUpdateInterval();

        if (updateIntervalTime > maxUpdateInterval && currentTime - printUpdateIntervalTime > Math.max(5000, maxUpdateInterval * 10)) {
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
            timerMgrSet.forEach(TimerMgr::update);
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
     * @param <R>        目标方法的返回结果泛型
     * @param proxy      服务代理
     * @param methodInfo 目标方法信息
     * @param params     方法参数列表
     */
    @SuppressWarnings("unchecked")
    protected <R> Promise<R> sendRequest(Proxy proxy, MethodInfo methodInfo, Object... params) {
        long callId = (long) this.id << 32 | getCallId();

        Promise<Object> promise = new Promise<>(this);
        promise.setCallId(callId);
        promise.setMethodInfo(methodInfo.getLabel());
        promise.calcExpiredTime(methodInfo.getExpiredTime());

        mappedPromises.put(callId, promise);
        sortedPromises.add(promise);

        sendRequest(proxy, promise, methodInfo, params);

        return (Promise<R>) promise;
    }

    private void sendRequest(Proxy proxy, Promise<Object> promise, MethodInfo methodInfo, Object... params) {
        int targetNodeId;
        Object serviceId;

        try {
            targetNodeId = proxy.getNodeId$(this);
            serviceId = proxy.getServiceId$(this);
        } catch (Exception e) {
            handleSendRequestError(promise, e);
            return;
        }

        if (promise.isExpired()) {
            logger.error("发送RPC请求,已过期无需发送,targetNodeId:{},serviceId:{},methodInfo:{}", targetNodeId, serviceId, promise.getMethodInfo());
            return;
        }

        if (targetNodeId == -1) {
            //延迟重新发送
            timerMgr.newTimer(() -> sendRequest(proxy, promise, methodInfo, params), getNode().getConfig().getUpdateInterval());
            return;
        }

        try {
            promise.setNodeId(targetNodeId);
            makeSafeParams(targetNodeId, methodInfo.isSafeArgs(), params);
            Request request = new Request(node.getId(), promise.getCallId(), serviceId, methodInfo.getId(), params);
            node.sendRequest(targetNodeId, request, methodInfo.isSafeReturn());
        } catch (Exception e) {
            handleSendRequestError(promise, e);
        }
    }

    private void handleSendRequestError(Promise<Object> promise, Exception e) {
        mappedPromises.remove(promise.getCallId());
        sortedPromises.remove(promise);
        CallException callException = e instanceof CallException ? (CallException) e : new CallException(e, CallException.Reason.SEND_ERROR);
        execute(() -> promise.setException(callException));
    }

    /**
     * 如果方法有参数是不安全的,则需要复制它以保证安全
     *
     * @param safeArgs 1:参考 {@link Endpoint#safeArgs()}
     */
    private void makeSafeParams(int targetNodeId, boolean safeArgs, Object[] params) {
        if (params == null || targetNodeId != 0 && targetNodeId != this.node.getId() || safeArgs) {
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
     * @param safeReturn 2:参考 {@link Endpoint#safeReturn()}
     */
    private Object makeSafeResult(int originNodeId, boolean safeReturn, Object result) {
        if (result == null || originNodeId != this.node.getId()) {
            return result;
        }

        if (ConstantUtils.isConstant(result) || safeReturn) {
            return result;
        } else {
            return cloneObject(result);
        }
    }

    protected Object cloneObject(Object object) {
        return SerializeUtils.clone(object, true);
    }

    protected void handleRequest(Request request, boolean safeReturn) {
        execute(() -> {
            try {
                handleRequest0(request, safeReturn);
            } catch (Exception e) {
                logger.error("处理RPC请求出错,callId:{},originNodeId:{}", request.getCallId(), request.getOriginNodeId());
            }
        });
    }

    private void handleRequest0(Request request, boolean safeReturn) {
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
            logger.error("处理RPC请求,方法执行出错,callId:{},originNodeId:{}", callId, originNodeId, e);
            if (originNodeId != this.node.getId() && this.node.getConfig().isThrowExceptionToRemote()) {
                node.sendResponse(originNodeId, callId, null, e);
            } else {
                node.sendResponse(originNodeId, callId, null, e.toString());
            }
            return;
        }

        if (!(result instanceof Promise)) {
            node.sendResponse(originNodeId, callId, result, null);
            return;
        }

        Promise<?> promise = (Promise<?>) result;

        if (promise instanceof DelayedResult) {
            if (promise.getMethodInfo() == null) {
                promise.setMethodInfo(invoker.getMethodLabel(methodId));
                int expiredTime = invoker.getExpiredTime(methodId);
                if (expiredTime > 0) {
                    promise.calcExpiredTime(expiredTime);
                    //更新基于过期时间的排序
                    sortedPromises.remove(promise);
                    sortedPromises.add(promise);
                }
            } else {
                logger.error("处理RPC请求,方法不能返回已被使用过的{},methodInfo:", promise.getClass().getSimpleName(), invoker.getMethodLabel(methodId));
            }
        }

        promise.completely0((r, e) -> {
            if (promise instanceof DelayedResult && !sortedPromises.remove(promise)) {
                return;
            }

            Object safeResult = makeSafeResult(originNodeId, safeReturn, promise.getResult());

            if (promise.isOK()) {
                node.sendResponse(originNodeId, callId, safeResult, null);
            } else if (originNodeId != this.node.getId() && this.node.getConfig().isThrowExceptionToRemote()) {
                node.sendResponse(originNodeId, callId, safeResult, promise.getException());
            } else {
                node.sendResponse(originNodeId, callId, safeResult, promise.getExceptionStr());
            }
        });
    }

    public <R> DelayedResult<R> newDelayedResult() {
        DelayedResult<R> delayedResult = new DelayedResult<>(this);
        sortedPromises.add(delayedResult);
        return delayedResult;
    }

    protected void handleResponse(Response response) {
        execute(() -> {
            try {
                handleResponse0(response);
            } catch (Exception e) {
                logger.error("处理RPC响应出错,callId:{},originNodeId:{}", response.getCallId(), response.getOriginNodeId());
            }
        });
    }

    private void handleResponse0(Response response) {
        long callId = response.getCallId();
        if (!mappedPromises.containsKey(callId)) {
            logger.error("处理RPC响应,调用不存在,callId:{},originNodeId:{}", callId, response.getOriginNodeId());
        } else {
            Object exception = response.getException();
            if (exception == null) {
                handleResponse(callId, response.getResult(), null);
            } else {
                Promise<?> promise = mappedPromises.get(callId);
                CallException callException = exception instanceof Throwable ?
                        new CallException((Throwable) exception, CallException.Reason.REMOTE_ERROR) :
                        new CallException(exception.toString(), CallException.Reason.REMOTE_ERROR);
                promise.setCallInfo(callException);
                handleResponse(callId, null, callException);
            }
        }
    }

    protected void handleResponse(long callId, Object result, Exception exception) {
        @SuppressWarnings("unchecked")
        Promise<Object> promise = (Promise<Object>) mappedPromises.remove(callId);

        if (promise == null) {
            if (exception != null) {
                logger.error("处理RPC响应,调用方法出错,callId:{}", callId, exception);
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
