package quan.rpc;

public abstract class Proxy {

    /**
     * 目标节点ID
     */
    private int nodeId = -1;

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

    public Proxy(Object serviceId) {
        this.serviceId = serviceId;
    }

    public abstract String _getServiceName$();

    //方法名加特殊字符，避免和服务方法同名
    protected <R> Promise<R> _sendRequest$(String signature, int securityModifier, int methodId, Object... params) {
        Worker worker = Worker.current();
        if (worker == null) {
            throw new IllegalStateException("当前所处线程不合法");
        }
        if (nodeId < 0) {
            nodeId = worker.resolveTargetNodeId(this);
        }
        return worker.sendRequest(nodeId, serviceId, signature, securityModifier, methodId, params);
    }

}
