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
     * 构造方法1
     */
    public Proxy() {
    }

    /**
     * 构造方法2
     */
    public Proxy(int nodeId) {
        _setNodeId$(nodeId);
    }

    /**
     * 构造方法3
     */
    public Proxy(Object serviceId) {
        _setServiceId$(serviceId);
    }

    /**
     * 构造方法4
     */
    public Proxy(int nodeId, Object serviceId) {
        _setNodeId$(nodeId);
        _setServiceId$(serviceId);
    }

    /**
     * 构造方法5
     */
    public Proxy(NodeIdResolver nodeIdResolver) {
        _setNodeIdResolver$(nodeIdResolver);
    }

    /**
     * 构造方法6
     */
    public Proxy(NodeIdResolver nodeIdResolver, Object serviceId) {
        _setNodeIdResolver$(nodeIdResolver);
        _setServiceId$(serviceId);
    }

    protected final void _setNodeId$(int nodeId) {
        if (nodeId < 0) {
            throw new IllegalArgumentException("目标节点ID不能小于0");
        }
        this.nodeId = nodeId;
    }

    protected final void _setServiceId$(Object serviceId) {
        this.serviceId = Objects.requireNonNull(serviceId);
    }

    protected final void _setNodeIdResolver$(NodeIdResolver nodeIdResolver) {
        this.nodeIdResolver = Objects.requireNonNull(nodeIdResolver);
    }

    protected final int _getNodeId$(Worker worker) {
        if (nodeId >= 0) {
            return nodeId;
        }

        if (nodeIdResolver != null) {
            int _nodeId = nodeIdResolver.resolveNodeId(this);
            if (_nodeId >= -1) {
                return _nodeId;
            }
        }

        NodeIdResolver _nodeIdResolver = worker.getNode().getConfig().getNodeIdResolver();
        if (_nodeIdResolver != null) {
            int _nodeId = nodeIdResolver.resolveNodeId(this);
            if (_nodeId >= -1) {
                return _nodeId;
            }
        }

        //代表当前节点
        return 0;
    }

    protected final Object _getServiceId$(Worker worker) {
        if (serviceId != null) {
            return serviceId;
        }

        ServiceIdResolver serviceIdResolver = worker.getNode().getConfig().getServiceIdResolver();
        if (serviceIdResolver != null) {
            Object _serviceId = serviceIdResolver.resolveServiceId(this);
            if (_serviceId != null) {
                return _serviceId;
            }
        }

        return _getServiceName$();
    }

    protected <R> Promise<R> _sendRequest$(int methodId, String methodLabel, int methodSecurity, int expiredTime, Object... params) {
        Worker worker = Worker.current();
        if (worker == null) {
            throw new IllegalStateException("当前所处线程不合法");
        } else {
            return worker.sendRequest(this, methodId, methodLabel, methodSecurity, expiredTime, params);
        }
    }

    /**
     * 服务名
     */
    protected abstract String _getServiceName$();

}
