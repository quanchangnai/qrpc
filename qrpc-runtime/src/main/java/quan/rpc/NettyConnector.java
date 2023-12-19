package quan.rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.Protocol.Handshake;
import quan.rpc.Protocol.PingPong;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 基于Netty的网络连接器
 *
 * @author quanchangnai
 */
public class NettyConnector extends Connector {

    /**
     * 重连间隔时间(ms)
     */
    private int reconnectInterval = 5000;

    /**
     * 心跳间隔时间(ms)
     */
    private int pingPongInterval = 5000;

    private final Receiver receiver;

    private final Map<Integer, Sender> senders = new ConcurrentHashMap<>();

    private final Set<Integer> remoteIds = Collections.unmodifiableSet(senders.keySet());

    public NettyConnector(String ip, int port) {
        receiver = new Receiver(ip, port, this);
    }

    public void setReconnectInterval(int reconnectInterval) {
        if (reconnectInterval < 1000) {
            throw new IllegalArgumentException("重连间隔时间不能小于1000毫秒");
        }
        this.reconnectInterval = reconnectInterval;
    }

    public int getReconnectInterval() {
        return reconnectInterval;
    }

    public int getPingPongInterval() {
        return pingPongInterval;
    }

    public void setPingPongInterval(int pingPongInterval) {
        if (pingPongInterval < 1000) {
            throw new IllegalArgumentException("心跳间隔时间不能小于1000毫秒");
        }
        this.pingPongInterval = pingPongInterval;
    }

    @Override
    protected void start() {
        receiver.start();
        senders.values().forEach(Sender::start);
    }

    @Override
    protected void stop() {
        receiver.stop();
        senders.values().forEach(Sender::stop);
        senders.clear();
    }

    public boolean addRemote(int remoteId, String remoteIp, int remotePort) {
        if (remoteIds.contains(remoteId)) {
            logger.error("远程节点[{}]已存在", remoteId);
            return false;
        }

        Sender sender = new Sender(remoteId, remoteIp, remotePort, this);
        senders.put(remoteId, sender);

        if (node != null && node.isRunning()) {
            sender.start();
        }

        return true;
    }

    public boolean removeRemote(int remoteId) {
        Sender sender = senders.remove(remoteId);
        if (sender != null) {
            sender.stop();
        }
        return sender != null;
    }

    public Set<Integer> getRemoteIds() {
        return remoteIds;
    }

    public boolean isActivatedRemote(int remoteId) {
        Sender sender = senders.get(remoteId);
        return sender != null && sender.context != null;
    }

    @Override
    protected boolean isLegalRemote(int remoteId) {
        return remoteIds.contains(remoteId);
    }

    @Override
    protected void sendProtocol(int remoteId, Protocol protocol) {
        Sender sender = senders.get(remoteId);
        if (sender != null) {
            sender.sendProtocol(protocol);
        } else {
            throw new IllegalArgumentException(String.format("远程节点[%s]不存在", remoteId));
        }
    }

    protected void sendProtocol(ChannelHandlerContext context, Protocol protocol) {
        ByteBuf byteBuf = context.alloc().buffer();

        try {
            encode(protocol, new ByteBufOutputStream(byteBuf));
        } catch (Exception e) {
            byteBuf.release();
            throw new RuntimeException("序列化协议失败", e);
        }

        // TODO 刷新会执行系统调用，可能需要优化
        context.writeAndFlush(byteBuf);
    }

    /**
     * 处理RPC握手逻辑
     */
    protected boolean handleHandshake(Handshake handshake) {
        int remoteId = handshake.getOriginNodeId();
        if (!senders.containsKey(remoteId)) {
            if (!addRemote(remoteId, handshake.getIp(), handshake.getPort())) {
                return false;
            }
            senders.get(remoteId).passive = true;
        }
        return true;
    }

    protected void handlePingPong(PingPong pingPong) {
        Sender sender = senders.get(pingPong.getOriginNodeId());
        if (sender != null) {
            sender.handlePingPong(pingPong);
        }
        pingPong.setTime(System.currentTimeMillis());
    }

    @Override
    public String toString() {
        return getClass().getName() + "{ip=" + receiver.ip + ",port" + receiver.port + "}";
    }

    /**
     * 用于接收远程节点的数据
     */
    private static class Receiver {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        private final NettyConnector connector;

        private final String ip;

        private final int port;

        private ServerBootstrap serverBootstrap;

        public Receiver(String ip, int port, NettyConnector connector) {
            this.ip = ip;
            this.port = port;
            this.connector = connector;
        }

        public void start() {
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup(connector.node.getConfig().getIoThreadNum());

            serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new LengthFieldBasedFrameDecoder(100000, 0, 4, 0, 4));
                            p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {

                                @Override
                                protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) {
                                    Protocol protocol;
                                    try {
                                        protocol = connector.decode(new ByteBufInputStream(msg));
                                    } catch (Exception e) {
                                        logger.error("反序列化协议失败", e);
                                        return;
                                    }

                                    if (protocol instanceof Handshake) {
                                        if (!connector.handleHandshake((Handshake) protocol)) {
                                            context.close();
                                        }
                                    } else if (protocol instanceof PingPong) {
                                        connector.handlePingPong((PingPong) protocol);
                                        connector.sendProtocol(context, protocol);
                                    } else {
                                        connector.handleProtocol(protocol);
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    logger.error("网络连接异常", cause);
                                }

                            });
                        }
                    });

            serverBootstrap.bind(ip, port);
        }

        public void stop() {
            if (serverBootstrap != null) {
                serverBootstrap.config().group().shutdownGracefully();
                serverBootstrap.config().childGroup().shutdownGracefully();
                serverBootstrap = null;
            }
        }
    }

    /**
     * 用于向远程节点发送数据
     */
    private static class Sender {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        //远程节点ID
        private final int id;

        private final String ip;

        private final int port;

        protected NettyConnector connector;

        //被动添加的
        private boolean passive;

        private long lastSendPingPongTime;

        private long lastHandlePingPongTime;

        private long lastReportSuspendedTime;

        private final int reportSuspendedInterval;

        private Bootstrap bootstrap;

        private ChannelHandlerContext context;

        protected Sender(int id, String ip, int port, NettyConnector connector) {
            Validate.isTrue(id > 0, "节点ID必须是正整数");
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.connector = connector;
            this.reportSuspendedInterval = connector.getPingPongInterval() * 2;
        }

        protected void start() {
            EventLoopGroup group = new NioEventLoopGroup(1);
            bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new LengthFieldBasedFrameDecoder(100000, 0, 4, 0, 4));
                            p.addLast(new ChannelInboundHandlerAdapter() {

                                @Override
                                public void channelActive(ChannelHandlerContext context) {
                                    onActive(context);
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext context) {
                                    onInactive(context);
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    logger.error("网络连接异常", cause);
                                }

                            });
                        }
                    });

            group.scheduleAtFixedRate(this::update, 0, connector.node.getConfig().getUpdateInterval(), TimeUnit.MILLISECONDS);

            connect();
        }

        protected void stop() {
            if (bootstrap != null) {
                bootstrap.config().group().shutdownGracefully();
                bootstrap = null;
            }
        }

        protected void connect() {
            ChannelFuture channelFuture = bootstrap.connect(ip, port);
            channelFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    logger.error("连接远程节点[{}]失败，将在{}毫秒后尝试重连，失败原因：{}", id, connector.getReconnectInterval(), future.cause().getMessage());
                    reconnect();
                }
            });
        }

        protected void reconnect() {
            if (bootstrap != null) {
                bootstrap.config().group().schedule(this::connect, connector.getReconnectInterval(), TimeUnit.MILLISECONDS);
            }
        }

        protected void onActive(ChannelHandlerContext context) {
            this.context = context;
            Handshake handshake = new Handshake(connector.node.getId(), connector.receiver.ip, connector.receiver.port);
            sendProtocol(handshake);
        }

        protected void onInactive(ChannelHandlerContext context) {
            this.context = null;
            this.lastHandlePingPongTime = 0;

            if (passive) {
                logger.error("远程节点[{}]连接已断开：{}", id, context.channel().remoteAddress());
                connector.removeRemote(id);
            } else if (bootstrap != null) {
                logger.error("远程节点[{}]连接已断开，将在{}毫秒后尝试重连: {}", id, connector.getReconnectInterval(), context.channel().remoteAddress());
                reconnect();
            }
        }

        protected void sendProtocol(Protocol protocol) {
            if (context == null) {
                throw new IllegalStateException(String.format("远程节点[%s]的连接还未建立", id));
            } else {
                try {
                    connector.sendProtocol(context, protocol);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("发送协议到远程节点[%s]出错", id), e);
                }
            }
        }

        protected void update() {
            checkSuspended();
            sendPingPong();
        }

        protected void checkSuspended() {
            if (context == null || lastHandlePingPongTime <= 0) {
                return;
            }

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastHandlePingPongTime > reportSuspendedInterval && currentTime - lastReportSuspendedTime > reportSuspendedInterval) {
                logger.error("远程节点[{}]的连接可能已经进入假死状态了", this.id);
                lastReportSuspendedTime = currentTime;
            }
        }

        protected void sendPingPong() {
            if (context == null) {
                return;
            }

            long currentTime = System.currentTimeMillis();
            if (lastSendPingPongTime + connector.getPingPongInterval() < currentTime) {
                lastSendPingPongTime = currentTime;
                try {
                    sendProtocol(new PingPong(connector.node.getId(), currentTime));
                } catch (Exception e) {
                    logger.error("发送协议出错", e);
                }
            }
        }

        protected void handlePingPong(PingPong pingPong) {
            lastHandlePingPongTime = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("远程节点[{}]的延迟时间为{}毫秒", id, lastHandlePingPongTime - pingPong.getTime());
            }
        }

    }

}
