package quan.rpc;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import quan.rpc.Protocol.Request;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * 基于RabbitMQ的网络连接器
 *
 * @author quanchangnai
 */
public class RabbitConnector extends Connector {

    private String namePrefix = "";

    private final ConnectionFactory connectionFactory;

    private Connection connection;

    private Predicate<Integer> remoteChecker;

    private ThreadLocal<Channel> localChannel;

    private ScheduledExecutorService executor;

    /**
     * 构造基于基于RabbitMQ的网络连接器
     *
     * @param connectionFactory RabbitMQ连接工厂
     * @param namePrefix        RabbitMQ交换机和队列的名称前缀，需要互连的节点一定要保持一致
     */
    public RabbitConnector(ConnectionFactory connectionFactory, String namePrefix) {
        connectionFactory.useNio();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setNetworkRecoveryInterval(Math.max(connectionFactory.getNetworkRecoveryInterval(), 1000));

        this.connectionFactory = connectionFactory;

        if (namePrefix != null) {
            this.namePrefix = namePrefix;
        }
    }

    /**
     * @see #RabbitConnector(ConnectionFactory, String)
     */
    public RabbitConnector(ConnectionFactory connectionFactory) {
        this(connectionFactory, null);
    }

    public void setRemoteChecker(Predicate<Integer> remoteChecker) {
        this.remoteChecker = remoteChecker;
    }

    protected void start() {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("rabbit-connector-thread-%d").build();
        executor = Executors.newScheduledThreadPool(node.getConfig().getIoThreadNum(), threadFactory);
        localChannel = ThreadLocal.withInitial(this::initChannel);
        executor.execute(this::connect);
    }

    protected void stop() {
        localChannel = null;
        if (connection != null) {
            connection.abort();
            connection = null;
        }
        executor.shutdown();
        executor = null;
    }

    private void connect() {
        try {
            connection = connectionFactory.newConnection(executor);
        } catch (Exception e) {
            long reconnectInterval = connectionFactory.getNetworkRecoveryInterval();
            logger.error("连接RabbitMQ失败,将在{}毫秒后尝试重连", reconnectInterval, e);
            executor.schedule(this::connect, reconnectInterval, TimeUnit.MILLISECONDS);
            return;
        }

        //默认创建一个Channel
        getChannel();
    }

    protected String exchangeName(int nodeId) {
        return namePrefix + nodeId;
    }

    protected String queueName(int nodeId) {
        return namePrefix + nodeId;
    }

    private Channel getChannel() {
        if (connection == null) {
            throw new IllegalStateException("RabbitMQ连接还未建立");
        }

        Channel channel = localChannel.get();
        if (!channel.isOpen()) {
            localChannel.remove();
            channel = localChannel.get();
        }

        return channel;
    }

    private Channel initChannel() {
        try {
            Channel channel = connection.createChannel();
            channel.addShutdownListener(cause -> {
                //主动关闭连接
                boolean connectionClose = cause.getReason() instanceof AMQP.Connection.Close;
                if (cause.isHardError() && !connectionClose) {
                    logger.error("RabbitMQ connection shutdown", cause);
                } else if (!cause.isHardError() && !connectionClose) {
                    logger.error("RabbitMQ channel shutdown", cause);
                    //保证至少有一个channel
                    executor.execute(this::getChannel);
                }
            });

            String exchangeName = exchangeName(node.getId());
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);

            String queueName = queueName(node.getId());
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-message-ttl", node.getConfig().getMaxCallTtl());//设置队列里消息的过期时间
            channel.queueDeclare(queueName, false, true, true, queueArgs);

            channel.queueBind(queueName, exchangeName, "");

            channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    Protocol protocol;

                    try {
                        protocol = decode(new ByteArrayInputStream(body));
                    } catch (Exception e) {
                        logger.error("反序列化协议失败", e);
                        return;
                    }

                    try {
                        handleProtocol(protocol);
                    } catch (Exception e) {
                        logger.error("处理协议出错", e);
                    }
                }
            });

            return channel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean isLegalRemote(int remoteId) {
        return remoteChecker == null || remoteChecker.test(remoteId);
    }

    @Override
    protected void sendProtocol(int remoteId, Protocol protocol) {
        Worker worker = Worker.current();
        //异步发送，防止阻塞线程工作者
        executor.execute(() -> {
            try {
                byte[] bytes = encode(protocol);
                //exchange不存在时不会报错，会异步关闭channel
                getChannel().basicPublish(exchangeName(remoteId), "", null, bytes);
            } catch (Exception e) {
                String error = String.format("发送协议到远程节点[%s]出错", remoteId);
                if (protocol instanceof Request) {
                    long callId = ((Request) protocol).getCallId();
                    CallException callException = new CallException(error, e);
                    worker.execute(() -> worker.handleResponse(callId, null, callException));
                } else {
                    logger.error("{}:{}", error, protocol, e);
                }
            }
        });
    }


    @Override
    public String toString() {
        return getClass().getName() + "{ip=" + connectionFactory.getHost() + ",port" + connectionFactory.getPort() + "}";
    }

}
