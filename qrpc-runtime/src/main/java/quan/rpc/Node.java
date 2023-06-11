package quan.rpc;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.message.CodedBuffer;
import quan.rpc.protocol.Protocol;
import quan.rpc.protocol.Request;
import quan.rpc.protocol.Response;
import quan.rpc.serialize.ObjectReader;
import quan.rpc.serialize.ObjectWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RPC节点
 *
 * @author quanchangnai
 */
public class Node {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final int id;

    //刷帧的间隔时间(毫秒)
    private int updateInterval = 50;

    private long updateTime;

    //调用的超时时间(秒)
    private int callTtl = 10;

    private Function<CodedBuffer, ObjectReader> readerFactory = ObjectReader::new;

    private Function<CodedBuffer, ObjectWriter> writerFactory = ObjectWriter::new;

    private LinkedHashSet<Connector> connectors = new LinkedHashSet<>();

    private NodeIdResolver targetNodeIdResolver;

    //管理的所有工作线程，key:工作线程ID
    private Map<Integer, Worker> workers = new HashMap<>();

    private final List<Integer> workerIds = new ArrayList<>();

    private int workerIndex;

    //管理的所有服务，key:服务ID
    private final Map<Object, Service> services = new ConcurrentHashMap<>();

    private ScheduledExecutorService executor;

    private volatile boolean running;

    /**
     * @param id         节点ID
     * @param workerNum  工作线程数量
     * @param connectors 网络连接器，发送协议 {@link #sendProtocol(int, Protocol)}到远程节点时越靠前的{@link Connector}优先级越高
     */
    public Node(int id, int workerNum, Connector... connectors) {
        Validate.isTrue(id > 0, "节点ID必须是正整数");
        this.id = id;
        this.initWorkers(workerNum);

        for (Connector connector : connectors) {
            connector.node = this;
            this.connectors.add(connector);
        }
    }

    public Node(int id, Connector... connectors) {
        this(id, 0, connectors);
    }

    public Node(int id) {
        this(id, 0);
    }

    public final int getId() {
        return id;
    }

    public final Map<Integer, Worker> getWorkers() {
        return workers;
    }

    public final int getWorkerNum() {
        return workers.size();
    }

    /**
     * 设置刷帧的间隔时间(ms)
     */
    public void setUpdateInterval(int updateInterval) {
        if (updateInterval > 0) {
            this.updateInterval = updateInterval;
        }
    }

    public int getUpdateInterval() {
        return updateInterval;
    }

    public int getCallTtl() {
        return callTtl;
    }

    /**
     * 设置调用的超时时间(秒)
     */
    public void setCallTtl(int callTtl) {
        if (callTtl > 0) {
            this.callTtl = callTtl;
        }
    }

    /**
     * 设置{@link ObjectReader}工厂，用于扩展对象序列化
     */
    public void setReaderFactory(Function<CodedBuffer, ObjectReader> readerFactory) {
        this.readerFactory = Objects.requireNonNull(readerFactory);
    }

    public Function<CodedBuffer, ObjectReader> getReaderFactory() {
        return readerFactory;
    }

    /**
     * 设置{@link ObjectWriter}工厂，用于扩展对象序列化
     */
    public void setWriterFactory(Function<CodedBuffer, ObjectWriter> writerFactory) {
        this.writerFactory = Objects.requireNonNull(writerFactory);
    }

    public Function<CodedBuffer, ObjectWriter> getWriterFactory() {
        return writerFactory;
    }


    public void setTargetNodeIdResolver(NodeIdResolver targetNodeIdResolver) {
        this.targetNodeIdResolver = targetNodeIdResolver;
    }

    public NodeIdResolver getTargetNodeIdResolver() {
        return targetNodeIdResolver;
    }

    private void initWorkers(int workerNum) {
        if (workerNum <= 0) {
            workerNum = Runtime.getRuntime().availableProcessors();
        }

        for (int i = 0; i < workerNum; i++) {
            Worker worker = new Worker(this);
            workers.put(worker.getId(), worker);
        }

        workers = Collections.unmodifiableMap(workers);
        workerIds.addAll(workers.keySet());
    }

    private Worker nextWorker() {
        int workerId = workerIds.get(workerIndex++);
        if (workerIndex >= workerIds.size()) {
            workerIndex = 0;
        }
        return workers.get(workerId);
    }


    public void start() {
        try {
            workers.values().forEach(Worker::start);
            executor = Executors.newScheduledThreadPool(1, r -> new Thread(r, "node-thread"));
            executor.scheduleAtFixedRate(this::update, updateInterval, updateInterval, TimeUnit.MILLISECONDS);
            connectors.forEach(Connector::start);
        } finally {
            running = true;
        }
    }

    public void stop() {
        running = false;
        executor.shutdown();
        executor = null;
        connectors.forEach(Connector::stop);
        workers.values().forEach(Worker::stop);
        workers = new HashMap<>();
        workerIds.clear();
    }

    public boolean isRunning() {
        return running;
    }

    protected void update() {
        if (!running) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        long actualInterval = currentTime - updateTime;

        if (updateTime > 0) {
            if (actualInterval > updateInterval * 2L) {
                logger.error("实际刷帧间隔时间({})过长", actualInterval);
            } else if (actualInterval > updateInterval * 1.5) {
                logger.warn("实际刷帧间隔时间({})偏长", actualInterval);
            }
        }

        updateTime = currentTime;

        try {
            workers.values().forEach(Worker::update);
        } catch (Exception e) {
            logger.error("", e);
        }

    }

    public void addService(Service service) {
        addService(nextWorker(), service);
    }

    public void addService(Worker worker, Service service) {
        if (workers.get(worker.getId()) != worker) {
            throw new IllegalArgumentException(String.format("参数[worker]不是节点[%s]管理的工作线程", this.id));
        }

        Object serviceId = Objects.requireNonNull(service.getId(), "服务ID不能为空");
        if (services.putIfAbsent(serviceId, service) == null) {
            worker.run(() -> worker.doAddService(service));
        } else {
            logger.error("服务[{}]已存在", serviceId);
        }
    }

    public void removeService(Object serviceId) {
        Service service = services.remove(serviceId);
        if (service == null) {
            logger.error("服务[{}]不存在", serviceId);
            return;
        }

        Worker worker = service.getWorker();
        worker.run(() -> worker.doRemoveService(service));
    }

    protected void sendProtocol(int remoteId, Protocol protocol) {
        for (Connector connector : connectors) {
            if (connector.isLegalRemote(remoteId)) {
                connector.sendProtocol(remoteId, protocol);
                return;
            }
        }
        throw new IllegalArgumentException(String.format("远程节点[%s]不存在", remoteId));
    }

    /**
     * 发送RPC请求
     */
    protected void sendRequest(int targetNodeId, Request request, int securityModifier) {
        if (targetNodeId == this.id || targetNodeId == 0) {
            //本地节点直接处理
            handleRequest(request, securityModifier);
        } else {
            try {
                sendProtocol(targetNodeId, request);
            } catch (Exception e) {
                throw new CallException(String.format("发送协议到远程节点[%s]出错", targetNodeId), e);
            }
        }
    }

    /**
     * 处理RPC请求
     */
    protected void handleRequest(Request request, int securityModifier) {
        Service service = services.get(request.getServiceId());
        if (service == null) {
            logger.error("处理RPC请求，服务[{}]不存在", request.getServiceId());
        } else {
            Worker worker = service.getWorker();
            worker.run(() -> worker.handleRequest(request, securityModifier));
        }
    }

    protected void handleRequest(Request request) {
        handleRequest(request, 0b11);
    }

    /**
     * 发送RPC响应
     */
    protected void sendResponse(int targetNodeId, Response response) {
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
            logger.error("处理RPC响应，工作线程[{}]不存在，originNodeId:{}，callId:{}", workerId, response.getOriginNodeId(), callId);
        } else {
            worker.run(() -> worker.handleResponse(response));
        }
    }

}
