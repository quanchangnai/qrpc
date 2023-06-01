package quan.rpc.test;

import quan.rpc.NettyConnector;
import quan.rpc.Node;

/**
 * @author quanchangnai
 */
public class RpcTestNetty {

    public static void main(String[] args) {
        NettyConnector nettyConnector = new NettyConnector("127.0.0.1", 9999);
        Node node = new Node(2, 5, nettyConnector);
        nettyConnector.addRemote(1, "127.0.0.1", 8888);
        node.addService(new TestService2(2));
        node.start();
    }

}
