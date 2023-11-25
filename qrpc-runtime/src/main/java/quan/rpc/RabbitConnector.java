package quan.rpc;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import quan.message.CodedBuffer;
import quan.message.DefaultCodedBuffer;
import quan.rpc.protocol.Protocol;
import quan.rpc.protocol.Request;
import quan.rpc.serialize.ObjectWriter;

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

    private ThreadLocal<Channel> channelHolder;

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
        executor = Executors.newScheduledThreadPool(node.getConfig().getWorkerNum(), threadFactory);
        channelHolder = ThreadLocal.withInitial(this::initChannel);
        executor.execute(this::connect);
    }

    protected void stop() {
        channelHolder = null;
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
            logger.error("连接RabbitMQ失败，将在{}毫秒后尝试重连", reconnectInterval, e);
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
            throw new RuntimeException("RabbitMQ连接还未建立");
        }
        Channel channel = channelHolder.get();
        if (!channel.isOpen()) {
            channelHolder.remove();
            channel = channelHolder.get();
        }
        return channel;
    }

    private Channel initChannel() {
        try {
            Channel channel = connection.createChannel();
            channel.addShutdownListener(cause -> {
                boolean connectionClose = cause.getReason() instanceof AMQP.Connection.Close;
                if (!cause.isHardError()) {
                    if (!connectionClose) {
                        logger.error("RabbitMQ channel shutdown, reason:{}", cause.getReason(), cause);
                    } else {
                        //保证至少有一个channel
                        executor.execute(this::getChannel);
                    }
                } else if (!connectionClose) {
                    logger.error("RabbitMQ connection shutdown,reason:{}", cause.getReason(), cause);
                }
            });

            String exchangeName = exchangeName(node.getId());
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);

            String queueName = queueName(node.getId());
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-message-ttl", node.getConfig().getCallTtl());//设置队列里消息的过期时间
            channel.queueDeclare(queueName, false, true, true, queueArgs);
            channel.queueBind(queueName, exchangeName, "");

            channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    try {
                        CodedBuffer buffer = new DefaultCodedBuffer(body);
                        Protocol protocol = node.getConfig().getReaderFactory().apply(buffer).read();
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
                CodedBuffer buffer = new DefaultCodedBuffer();
                ObjectWriter objectWriter = node.getConfig().getWriterFactory().apply(buffer);
                objectWriter.write(protocol);

                //exchange不存在时不会报错，会异步关闭channel
                getChannel().basicPublish(exchangeName(remoteId), "", null, buffer.remainingBytes());
            } catch (Exception e) {
                if (protocol instanceof Request) {
                    CallException callException = new CallException(String.format("发送协议到远程节点[%s]出错", remoteId), e);
                    long callId = ((Request) protocol).getCallId();
                    worker.execute(() -> worker.handlePromise(callId, callException, null));
                } else {
                    logger.error("发送协议出错，{}", protocol, e);
                }
            }
        });
    }


    @Override
    public String toString() {
        return getClass().getName() + "{ip=" + connectionFactory.getHost() + ",port" + connectionFactory.getPort() + "}";
    }
}
