package quan.rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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
import quan.message.CodedBuffer;
import quan.message.NettyCodedBuffer;
import quan.rpc.protocol.Handshake;
import quan.rpc.protocol.PingPong;
import quan.rpc.protocol.Protocol;
import quan.rpc.serialize.ObjectWriter;

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
            logger.error("远程服务器[{}]已存在", remoteId);
            return false;
        }

        Sender sender = new Sender(remoteId, remoteIp, remotePort, this);
        senders.put(remoteId, sender);

        if (localServer != null && localServer.isRunning()) {
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
            throw new IllegalArgumentException(String.format("远程服务器[%s]不存在", remoteId));
        }
    }

    protected void sendProtocol(ChannelHandlerContext context, Protocol protocol) {
        ByteBuf byteBuf = context.alloc().buffer();
        try {
            ObjectWriter objectWriter = localServer.getWriterFactory().apply(new NettyCodedBuffer(byteBuf));
            objectWriter.write(protocol);
            // TODO 刷新会执行系统调用，需要优化
            context.writeAndFlush(byteBuf);
        } catch (Throwable e) {
            byteBuf.release();
            throw new RuntimeException("发送协议出错", e);
        }
    }

    /**
     * 处理RPC握手逻辑
     */
    protected boolean handleHandshake(Handshake handshake) {
        int remoteId = handshake.getServerId();
        if (!senders.containsKey(remoteId)) {
            if (!addRemote(remoteId, handshake.getIp(), handshake.getPort())) {
                return false;
            }
            senders.get(remoteId).passive = true;
        }
        return true;
    }

    protected void handlePingPong(PingPong pingPong) {
        Sender sender = senders.get(pingPong.getServerId());
        if (sender != null) {
            sender.handlePingPong(pingPong);
        }
        pingPong.setTime(System.currentTimeMillis());
    }

    /**
     * 用于接收远程服务器的数据
     */
    @SuppressWarnings("NullableProblems")
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
            EventLoopGroup workerGroup = new NioEventLoopGroup(connector.localServer.getWorkerNum());

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
                            p.addLast(new ChannelInboundHandlerAdapter() {

                                @Override
                                public void channelRead(ChannelHandlerContext context, Object msg) {
                                    CodedBuffer buffer = new NettyCodedBuffer((ByteBuf) msg);
                                    Protocol protocol = connector.localServer.getReaderFactory().apply(buffer).read();

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
                                    logger.error("", cause);
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
     * 用于向远程服务器发送数据
     */
    @SuppressWarnings("NullableProblems")
    private static class Sender {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        //远程服务器ID
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
            Validate.isTrue(id > 0, "服务器ID必须是正整数");
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
                                    logger.error("", cause);
                                }

                            });
                        }
                    });

            group.scheduleAtFixedRate(this::update, 0, connector.localServer.getUpdateInterval(), TimeUnit.MILLISECONDS);

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
                    logger.error("连接远程服务器[{}]失败，将在{}毫秒后尝试重连，失败原因：{}", id, connector.getReconnectInterval(), future.cause().getMessage());
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
            Handshake handshake = new Handshake(connector.localServer.getId(), connector.receiver.ip, connector.receiver.port);
            sendProtocol(handshake);
        }

        protected void onInactive(ChannelHandlerContext context) {
            this.context = null;
            this.lastHandlePingPongTime = 0;

            if (passive) {
                logger.error("远程服务器[{}]连接已断开：{}", id, context.channel().remoteAddress());
                connector.removeRemote(id);
            } else if (bootstrap != null) {
                logger.error("远程服务器[{}]连接已断开，将在{}毫秒后尝试重连: {}", id, connector.getReconnectInterval(), context.channel().remoteAddress());
                reconnect();
            }
        }

        protected void sendProtocol(Protocol protocol) {
            if (context == null) {
                throw new IllegalStateException(String.format("远程服务器[%s]的连接还未建立", id));
            } else {
                try {
                    connector.sendProtocol(context, protocol);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("发送协议到远程服务器[%s]出错", id), e.getCause());
                }
            }
        }

        protected void update() {
            try {
                checkSuspended();
                sendPingPong();
            } catch (Exception e) {
                logger.error("", e);
            }
        }

        protected void checkSuspended() {
            if (context == null || lastHandlePingPongTime <= 0) {
                return;
            }

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastHandlePingPongTime > reportSuspendedInterval && currentTime - lastReportSuspendedTime > reportSuspendedInterval) {
                logger.error("远程服务器[{}]的连接可能已经进入假死状态了", this.id);
                lastReportSuspendedTime = currentTime;
            }
        }

        protected void sendPingPong() {
            if (context == null) {
                return;
            }

            long currentTime = System.currentTimeMillis();
            if (lastSendPingPongTime + connector.getPingPongInterval() < currentTime) {
                sendProtocol(new PingPong(connector.localServer.getId(), currentTime));
                lastSendPingPongTime = currentTime;
            }
        }

        protected void handlePingPong(PingPong pingPong) {
            lastHandlePingPongTime = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("远程服务器[{}]的延迟时间为{}毫秒", id, lastHandlePingPongTime - pingPong.getTime());
            }
        }

    }

}
