package quan.rpc;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.Protocol.Request;
import quan.rpc.Protocol.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * RPC节点
 *
 * @author quanchangnai
 */
public class Node {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final int id;

    private final Config config;

    private long timeOffset;

    private LinkedHashSet<Connector> connectors = new LinkedHashSet<>();

    //key:工作者ID
    private Map<Integer, Worker> workers = new HashMap<>();

    //管理的所有服务，key:服务ID
    private final Map<Object, Service<?>> services = new ConcurrentHashMap<>();

    private ScheduledExecutorService executor;

    private volatile boolean running;

    /**
     * 构造RPC节点对象
     *
     * @param id         节点ID
     * @param config     节点配置
     * @param connectors 网络连接器，发送协议 {@link #sendProtocol(int, Protocol)}到远程节点时越靠前的{@link Connector}优先级越高
     */
    public Node(int id, Config config, Connector... connectors) {
        Validate.isTrue(id > 0, "节点ID必须是正整数");
        this.id = id;
        this.config = config == null ? new Config() : config;
        this.config.validate();

        this.initWorkers();

        for (Connector connector : connectors) {
            connector.node = this;
            this.connectors.add(connector);
        }
    }

    public Node(int id, Connector... connectors) {
        this(id, null, connectors);
    }

    public final int getId() {
        return id;
    }

    public Config getConfig() {
        return config;
    }

    public final Map<Integer, Worker> getWorkers() {
        return workers;
    }

    /**
     * 增加时间偏移量(毫秒)
     */
    public void addTimeOffset(long timeOffset) {
        if (timeOffset < 0) {
            throw new IllegalArgumentException("时间偏移量不能小于0");
        }
        this.timeOffset += timeOffset;
    }

    public long getTimeOffset() {
        return timeOffset;
    }


    private void initWorkers() {
        for (int i = 0; i < config.singleThreadWorkerNum; i++) {
            Worker worker = new SingleThreadWorker(this);
            workers.put(worker.getId(), worker);
        }

        for (Config.ThreadPoolParam threadPoolParam : config.threadPoolParams) {
            Worker worker = new ThreadPoolWorker(this, threadPoolParam);
            workers.put(worker.getId(), worker);
        }

        workers = Collections.unmodifiableMap(workers);
    }

    public void start() {
        try {
            workers.values().forEach(Worker::start);
            executor = Executors.newScheduledThreadPool(1, r -> new Thread(r, "node-" + id));
            executor.scheduleAtFixedRate(this::update, config.updateInterval, config.updateInterval, TimeUnit.MILLISECONDS);
            connectors.forEach(Connector::start);
        } finally {
            running = true;
        }
    }

    public void stop() {
        running = false;
        executor.shutdown();
        for (Connector connector : connectors) {
            try {
                connector.stop();
            } catch (Exception e) {
                logger.error("{}关闭出错", connector, e);
            }
        }
        workers.values().forEach(Worker::stop);
    }

    public boolean isRunning() {
        return running;
    }

    protected void update() {
        if (!running) {
            return;
        }

        try {
            workers.values().forEach(Worker::update);
        } catch (Exception e) {
            logger.error("节点[]刷帧出错", id, e);
        }
    }

    public long getTime() {
        return System.currentTimeMillis() + timeOffset;
    }

    public void addService(Service<?> service) {
        Worker worker = (Worker) workers.values().toArray()[RandomUtils.nextInt(0, workers.size())];
        addService(service, worker);
    }

    public void addService(Service<?> service, int workerId) {
        Objects.requireNonNull(service, "服务不能为空");
        Object serviceId = Objects.requireNonNull(service.getId(), "服务ID不能为空");

        Worker worker = workers.get(workerId);
        if (worker == null) {
            throw new IllegalArgumentException(String.format("参数[workerId]不合法,不存在线程工作者:%s", workerId));
        }

        if (services.putIfAbsent(serviceId, service) == null) {
            worker.execute(() -> worker.doAddService(service));
        } else {
            logger.error("服务[{}]已存在", serviceId);
        }
    }

    public void addService(Service<?> service, Worker worker) {
        Objects.requireNonNull(service);
        Objects.requireNonNull(worker);

        if (workers.get(worker.getId()) != worker) {
            throw new IllegalArgumentException(String.format("参数[worker]不是节点[%s]管理的线程工作者", this.id));
        }

        addService(service, worker.getId());
    }

    public void addService(Service<?> service, Predicate<Worker> predicate) {
        List<Worker> list = workers.values().stream().filter(predicate).collect(Collectors.toList());
        if (!list.isEmpty()) {
            addService(service, list.get(RandomUtils.nextInt(0, list.size())));
        } else {
            throw new IllegalStateException("没有找到合适的线程工作者");
        }
    }

    public void removeService(Object serviceId) {
        Objects.requireNonNull(serviceId);

        Service<?> service = services.remove(serviceId);
        if (service == null) {
            logger.error("服务[{}]不存在", serviceId);
            return;
        }

        Worker worker = service.getWorker();
        worker.execute(() -> worker.doRemoveService(service));
    }

    protected void sendProtocol(int remoteId, Protocol protocol) {
        for (Connector connector : connectors) {
            if (connector.isLegalRemote(remoteId)) {
                if (connector.isRemoteConnected(remoteId)) {
                    connector.sendProtocol(remoteId, protocol);
                    return;
                } else {
                    throw new CallException(String.format("远程节点[%s]的连接未建立或者断了", remoteId), CallException.Reason.DISCONNECTED);
                }
            }
        }

        throw new CallException(String.format("远程节点[%s]不存在", remoteId), CallException.Reason.INVALID_NODE);
    }

    /**
     * 发送RPC请求
     */
    protected void sendRequest(int targetNodeId, Request request, boolean safeReturn) {
        if (targetNodeId == this.id || targetNodeId == 0) {
            //本地节点直接处理
            handleRequest(request, safeReturn);
        } else {
            sendProtocol(targetNodeId, request);
        }
    }

    /**
     * 处理RPC请求
     */
    protected void handleRequest(Request request, boolean safeReturn) {
        int originNodeId = request.getOriginNodeId();
        long callId = request.getCallId();
        Object serviceId = request.getServiceId();
        Service<?> service = services.get(serviceId);

        if (service == null) {
            logger.error("处理RPC请求,服务[{}]不存在,callId:{},originNodeId:{}", serviceId, callId, originNodeId);
            sendResponse(originNodeId, callId, null, String.format("服务[%s]不存在", serviceId));
        } else {
            service.getWorker().handleRequest(request, safeReturn);
        }
    }

    protected void handleRequest(Request request) {
        handleRequest(request, true);
    }

    /**
     * 发送RPC响应
     */
    protected void sendResponse(int targetNodeId, long callId, Object result, Object exception) {
        Response response = new Response(this.id, callId, result, exception);
        if (targetNodeId == this.id) {
            //本地节点直接处理
            handleResponse(response);
        } else {
            sendProtocol(targetNodeId, response);
        }
    }

    /**
     * 处理RPC响应
     */
    protected void handleResponse(Response response) {
        long callId = response.getCallId();
        int workerId = (int) (callId >> 32);
        Worker worker = workers.get(workerId);
        if (worker == null) {
            logger.error("处理RPC响应,线程工作者[{}]不存在,callId:{},originNodeId:{}", workerId, callId, response.getOriginNodeId());
        } else {
            worker.handleResponse(response);
        }
    }

    public static class Config {

        private boolean readonly;

        private int updateInterval = 50;

        /**
         * 最大刷帧等待时间(毫秒)，[刷帧任务开始执行的时间戳]减去[提交刷帧任务的时间戳]超过此值会打印日志，最好大于GC最大暂停时间
         */
        private int maxUpdateWaitTime = 200;

        /**
         * 最大刷帧消耗时间(毫秒)，[刷帧任务结束执行的时间戳]减去[刷帧任务开始执行的时间戳]超过此值会打印日志，最好大于GC最大暂停时间
         */
        private int maxUpdateCostTime = 200;

        /**
         * [当前时间戳]减去[刷帧任务开始执行的时间戳]超过此值会打印日志，一定要大于{@link #maxUpdateWaitTime}+{@link #maxUpdateCostTime}
         */
        private int maxUpdateInterval = 1000;

        private int callTtl = 30 * 1000;

        private int maxCallTtl = 5 * 60 * 1000;

        private int singleThreadWorkerNum = Runtime.getRuntime().availableProcessors();

        /**
         * 单线程工作者自定义标记前缀
         */
        private String singleThreadWorkerFlag = "";

        private List<ThreadPoolParam> threadPoolParams = new ArrayList<>();

        private int ioThreadNum = 4;

        private NodeIdResolver nodeIdResolver;

        private ServiceIdResolver serviceIdResolver;

        private boolean throwExceptionToRemote;


        private void checkReadonly() {
            if (readonly) {
                throw new IllegalStateException("当前状态不允许设置属性");
            }
        }

        private void validate() {
            Validate.isTrue(getWorkerNum() >= 1, "工作者数量不能小于1");
            Validate.isTrue(maxUpdateWaitTime > 0, "最大刷帧等待时间[%d]错误", maxUpdateWaitTime);
            Validate.isTrue(maxUpdateCostTime > 0, "最大刷帧消耗时间[%d]错误", maxUpdateCostTime);
            Validate.isTrue(maxUpdateInterval > maxUpdateWaitTime + maxUpdateCostTime, "最大刷帧间隔时间[%d]错误", maxUpdateInterval);
            Validate.isTrue(callTtl < maxCallTtl, "调用超时时间[%d]错误", callTtl);
            threadPoolParams = Collections.unmodifiableList(threadPoolParams);
            this.readonly = true;
        }

        public int getUpdateInterval() {
            return updateInterval;
        }

        /**
         * 设置刷帧的间隔时间(毫秒)
         */
        public Config setUpdateInterval(int updateInterval) {
            checkReadonly();
            Validate.isTrue(updateInterval >= 10, "刷帧的间隔时间不能小于10毫秒");
            this.updateInterval = updateInterval;
            return this;
        }

        public int getMaxUpdateWaitTime() {
            return maxUpdateWaitTime;
        }

        /**
         * 设置{@link Worker}的最大刷帧等待时间(毫秒)，{@link #maxUpdateWaitTime}
         */
        public Config setMaxUpdateWaitTime(int maxUpdateWaitTime) {
            checkReadonly();
            this.maxUpdateWaitTime = maxUpdateWaitTime;
            return this;
        }

        public int getMaxUpdateCostTime() {
            return maxUpdateCostTime;
        }

        /**
         * 设置{@link Worker}最大刷帧消耗时间(毫秒)，{@link #maxUpdateCostTime}
         */
        public Config setMaxUpdateCostTime(int maxUpdateCostTime) {
            checkReadonly();
            this.maxUpdateCostTime = maxUpdateCostTime;
            return this;
        }

        public int getMaxUpdateInterval() {
            return maxUpdateInterval;
        }

        /**
         * 设置{@link Worker}最大刷帧间隔时间(毫秒)，{@link #maxUpdateInterval}>{@link #maxUpdateWaitTime}+{@link #maxUpdateCostTime}
         */
        public Config setMaxUpdateInterval(int maxUpdateInterval) {
            checkReadonly();
            this.maxUpdateInterval = maxUpdateInterval;
            return this;
        }

        /**
         * 返回调用方法的超时时间(毫秒)
         */
        public int getCallTtl() {
            return callTtl;
        }

        /**
         * 设置调用方法的超时时间(秒)
         */
        public Config setCallTtl(int callTtl) {
            checkReadonly();
            Validate.isTrue(callTtl >= 1, "调用方法的超时时间不能小于1秒");
            this.callTtl = callTtl * 1000;
            return this;
        }

        /**
         * 返回调用方法的最大超时时间(毫秒)
         */
        public int getMaxCallTtl() {
            return maxCallTtl;
        }

        /**
         * 设置调用方法的最大超时时间(毫秒)
         */
        public Config setMaxCallTtl(int maxCallTtl) {
            checkReadonly();
            Validate.isTrue(maxCallTtl >= 10, "调用方法的最大超时时间不能小于10秒");
            this.maxCallTtl = maxCallTtl * 1000;
            return this;
        }

        public int getIoThreadNum() {
            return ioThreadNum;
        }

        /**
         * 设置网络IO线程数量
         */
        public Config setIoThreadNum(int ioThreadNum) {
            checkReadonly();
            this.ioThreadNum = ioThreadNum;
            Validate.isTrue(singleThreadWorkerNum >= 1, "网络IO线程数量不能小于1");
            return this;
        }

        public int getSingleThreadWorkerNum() {
            return singleThreadWorkerNum;
        }

        /**
         * 设置单线程工作者数量
         */
        public Config setSingleThreadWorkerNum(int singleThreadWorkerNum) {
            checkReadonly();
            Validate.isTrue(singleThreadWorkerNum >= 0, "单线程工作者数量不能小于0");
            this.singleThreadWorkerNum = singleThreadWorkerNum;
            return this;
        }

        public Config setSingleThreadWorkerFlag(String singleThreadWorkerFlag) {
            checkReadonly();
            if (singleThreadWorkerFlag != null) {
                this.singleThreadWorkerFlag = singleThreadWorkerFlag;
            }
            return this;
        }

        public String getSingleThreadWorkerFlag() {
            return singleThreadWorkerFlag;
        }

        /**
         * 设置线程池工作者参数
         *
         * @param corePoolSize   核心池大小
         * @param maxPoolSize    最大池大小
         * @param poolSizeFactor 池大小系数，当[已提交还未执行完的任务数量>当前池大小*池大小系数]时将创建新线程
         */
        public Config addThreadPoolWorker(int corePoolSize, int maxPoolSize, int poolSizeFactor, String flag) {
            checkReadonly();
            Validate.isTrue(corePoolSize >= 2 && maxPoolSize >= corePoolSize && poolSizeFactor > 0, "线程池工作者参数错误");
            threadPoolParams.add(new ThreadPoolParam(corePoolSize, maxPoolSize, poolSizeFactor, flag));
            return this;
        }

        public Config addThreadPoolWorker(int corePoolSize, int maxPoolSize, int poolSizeFactor) {
            return addThreadPoolWorker(corePoolSize, maxPoolSize, poolSizeFactor, null);
        }

        public Config addThreadPoolWorker(int corePoolSize, int maxPoolSize, String flag) {
            return addThreadPoolWorker(corePoolSize, maxPoolSize, 5, flag);
        }

        public Config addThreadPoolWorker(int corePoolSize, int maxPoolSize) {
            return addThreadPoolWorker(corePoolSize, maxPoolSize, 5, null);
        }

        public List<ThreadPoolParam> getThreadPoolParams() {
            return threadPoolParams;
        }

        /**
         * 工作者总数量
         */
        public int getWorkerNum() {
            return singleThreadWorkerNum + threadPoolParams.size();
        }

        public NodeIdResolver getNodeIdResolver() {
            return nodeIdResolver;
        }

        /**
         * 设置节点ID解析器
         */
        public Config setNodeIdResolver(NodeIdResolver nodeIdResolver) {
            checkReadonly();
            this.nodeIdResolver = Objects.requireNonNull(nodeIdResolver);
            return this;
        }

        public ServiceIdResolver getServiceIdResolver() {
            return serviceIdResolver;
        }

        /**
         * 设置服务ID解析器
         */
        public Config setServiceIdResolver(ServiceIdResolver serviceIdResolver) {
            checkReadonly();
            this.serviceIdResolver = Objects.requireNonNull(serviceIdResolver);
            return this;
        }

        public boolean isThrowExceptionToRemote() {
            return throwExceptionToRemote;
        }

        public Config setThrowExceptionToRemote(boolean throwExceptionToRemote) {
            checkReadonly();
            this.throwExceptionToRemote = throwExceptionToRemote;
            return this;
        }


        public static class ThreadPoolParam {

            private int corePoolSize;

            private int maxPoolSize;

            /**
             * 池大小系数，当[已提交还未执行完的任务数量>当前池大小*池大小系数]时将创建新线程
             */
            private int poolSizeFactor;

            /**
             * 线程池工作者自定义标记
             */
            private String flag = "";

            protected ThreadPoolParam(int corePoolSize, int maxThreadPoolSize, int threadPoolSizeFactor, String flag) {
                this.corePoolSize = corePoolSize;
                this.maxPoolSize = maxThreadPoolSize;
                this.poolSizeFactor = threadPoolSizeFactor;
                if (flag != null) {
                    this.flag = flag;
                }
            }

            public int getCorePoolSize() {
                return corePoolSize;
            }

            public int getMaxPoolSize() {
                return maxPoolSize;
            }

            public int getPoolSizeFactor() {
                return poolSizeFactor;
            }

            public String getFlag() {
                return flag;
            }
        }
    }

}
