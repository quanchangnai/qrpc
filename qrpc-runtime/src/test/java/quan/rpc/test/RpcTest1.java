package quan.rpc.test;

import com.rabbitmq.client.ConnectionFactory;
import quan.rpc.LocalServer;
import quan.rpc.NettyConnector;
import quan.rpc.RabbitConnector;

/**
 * @author quanchangnai
 */
public class RpcTest1 {

    public static void main(String[] args) {
        NettyConnector nettyConnector = new NettyConnector("127.0.0.1", 8888);

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        RabbitConnector rabbitConnector = new RabbitConnector(connectionFactory);

        LocalServer localServer = new LocalServer(1, nettyConnector, rabbitConnector);
        localServer.addService(new TestService1(1));
        localServer.addService(new RoleService1<>(2));
        localServer.start();
    }

}
