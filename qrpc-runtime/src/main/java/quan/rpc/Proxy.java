package quan.rpc;

import java.util.Objects;

/**
 * 服务代理，自身的方法名加特殊字符，避免和服务方法同名
 */
public abstract class Proxy {

    /**
     * 目标节点ID
     */
    private int nodeId = -1;

    /**
     * 目标服务ID
     */
    private Object serviceId;

    /**
     * 目标节点ID解析器
     */
    private NodeIdResolver nodeIdResolver;

    /**
     * 分片键
     */
    private Object shardingKey;


    protected final void setNodeId$(int nodeId) {
        if (nodeId < 0) {
            throw new IllegalArgumentException("节点ID不能小于0");
        }
        this.nodeId = nodeId;
    }

    protected final void setServiceId$(Object serviceId) {
        this.serviceId = Objects.requireNonNull(serviceId, "服务ID不能为空");
    }

    protected final void setNodeIdResolver$(NodeIdResolver nodeIdResolver) {
        this.nodeIdResolver = Objects.requireNonNull(nodeIdResolver, "节点ID解析器不能为空");
    }

    protected final void setShardingKey$(Object shardingKey) {
        this.shardingKey = Objects.requireNonNull(shardingKey, "分片键不能为空");
    }

    public final Object getShardingKey$() {
        return shardingKey;
    }

    protected final int getNodeId$(Worker worker) {
        if (nodeId >= 0) {
            return nodeId;
        }

        int _nodeId = getNodeId$(nodeIdResolver);
        if (_nodeId >= -1) {
            return _nodeId;
        }

        _nodeId = getNodeId$(worker.getNode().getConfig().getNodeIdResolver());
        if (_nodeId >= -1) {
            return _nodeId;
        }

        //代表当前节点
        return 0;
    }

    private int getNodeId$(NodeIdResolver nodeIdResolver) {
        if (nodeIdResolver == null) {
            return -2;
        }

        int _nodeId = nodeIdResolver.resolveNodeId(this);
        if (_nodeId < -1) {
            return -2;
        }

        if (nodeIdResolver.isCacheNodeId(this)) {
            this.nodeId = _nodeId;
        }

        return _nodeId;
    }

    protected final Object getServiceId$(Worker worker) {
        if (serviceId != null) {
            return serviceId;
        }

        ServiceIdResolver serviceIdResolver = worker.getNode().getConfig().getServiceIdResolver();

        if (serviceIdResolver != null) {
            Object _serviceId = serviceIdResolver.resolveServiceId(this);
            if (_serviceId != null) {
                if (serviceIdResolver.isCacheServiceId(this)) {
                    this.serviceId = _serviceId;
                }
                return _serviceId;
            }
        }

        return getServiceName$();
    }

    protected <R> Promise<R> sendRequest$(int methodId, String methodLabel, int methodSecurity, int expiredTime, Object... params) {
        Worker worker = Worker.current();
        if (worker == null) {
            throw new IllegalStateException("当前所处线程不合法," + Thread.currentThread());
        } else {
            return worker.sendRequest(this, methodId, methodLabel, methodSecurity, expiredTime, params);
        }
    }

    /**
     * 服务名
     */
    protected abstract String getServiceName$();

}
