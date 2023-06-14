package quan.rpc.test;

import com.rabbitmq.client.ConnectionFactory;
import quan.rpc.NettyConnector;
import quan.rpc.Node;
import quan.rpc.RabbitConnector;

/**
 * @author quanchangnai
 */

public class RpcTest1 {

    public static void main(String[] args) throws InterruptedException {
        NettyConnector nettyConnector = new NettyConnector("127.0.0.1", 8888);

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        RabbitConnector rabbitConnector = new RabbitConnector(connectionFactory);

        Node node = new Node(1, nettyConnector, rabbitConnector);
        node.addService(new TestService1(1));
        node.addService(new RoleService1<>(2));
        node.start();

    }

}
