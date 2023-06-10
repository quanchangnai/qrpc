package quan.rpc;

/**
 * 服务代理，自身的方法名加特殊字符，避免和服务方法同名
 */
public abstract class Proxy {

    /**
     * 目标节点ID
     */
    private int nodeId = -1;

    private NodeIdResolver nodeIdResolver;

    /**
     * 目标服务ID
     */
    private final Object serviceId;

    public Proxy(int nodeId, Object serviceId) {
        if (nodeId < 0) {
            throw new IllegalArgumentException("目标节点ID不能小于0");
        }
        this.nodeId = nodeId;
        this.serviceId = serviceId;
    }

    public Proxy(NodeIdResolver nodeIdResolver, Object serviceId) {
        this.nodeIdResolver = nodeIdResolver;
        this.serviceId = serviceId;
    }

    public Proxy(Object serviceId) {
        this.serviceId = serviceId;
    }

    public abstract String _getServiceName$();

    public int _getNodeId$() {
        return nodeId;
    }

    protected NodeIdResolver _getNodeIdResolver$() {
        return nodeIdResolver;
    }

    public Object _getServiceId() {
        return serviceId;
    }

    protected <R> Promise<R> _sendRequest$(String signature, int securityModifier, int methodId, Object... params) {
        Worker worker = Worker.current();
        if (worker == null) {
            throw new IllegalStateException("当前所处线程不合法");
        } else {
            return worker.sendRequest(this, signature, securityModifier, methodId, params);
        }
    }

}
