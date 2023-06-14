package quan.rpc;

/**
 * 节点ID解析器
 *
 * @author quanchangnai
 */
public interface NodeIdResolver {

    /**
     * 解析服务代理的目标节点ID
     *
     * @return -1：需要取消当前RPC请求，然后延迟重新发送；0：相当于当前节点ID；大于0：实际的节点ID
     */
    int resolve(Proxy proxy);

}
