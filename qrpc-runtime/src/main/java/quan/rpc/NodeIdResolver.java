package quan.rpc;

/**
 * 节点ID解析器
 *
 * @author quanchangnai
 */
public interface NodeIdResolver {

    /**
     * 解析目标节点ID
     *
     * @param proxy 服务代理
     * @return 小于-1:无效返回值,-1：需要取消当前RPC请求，然后延迟重新发送；0：相当于当前节点ID；大于0：实际的节点ID
     */
    int resolveNodeId(Proxy proxy);

}
