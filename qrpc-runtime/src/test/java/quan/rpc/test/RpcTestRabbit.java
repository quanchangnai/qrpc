package quan.rpc.test;

import com.rabbitmq.client.ConnectionFactory;
import quan.rpc.Node;
import quan.rpc.RabbitConnector;

/**
 * @author quanchangnai
 */
public class RpcTestRabbit {

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        RabbitConnector rabbitConnector = new RabbitConnector(connectionFactory);

        Node node = new Node(3, rabbitConnector);

        node.addService(new TestService2(2));

        node.start();
    }

}
