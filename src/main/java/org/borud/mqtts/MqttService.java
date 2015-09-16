package org.borud.mqtts;

import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;

import static io.netty.handler.codec.mqtt.MqttMessageType.CONNACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.CONNECT;
import static io.netty.handler.codec.mqtt.MqttMessageType.DISCONNECT;
import static io.netty.handler.codec.mqtt.MqttMessageType.PINGREQ;
import static io.netty.handler.codec.mqtt.MqttMessageType.PINGRESP;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBCOMP;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBLISH;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREC;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREL;
import static io.netty.handler.codec.mqtt.MqttMessageType.SUBACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.SUBSCRIBE;
import static io.netty.handler.codec.mqtt.MqttMessageType.UNSUBACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.UNSUBSCRIBE;

import io.netty.util.AttributeKey;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.TimeUnit;

import java.net.InetSocketAddress;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Mqtt Service.  This class is immutable.
 *
 * @author borud
 */
@Sharable
public final class MqttService // extends ChannelInboundHandlerAdapter {
{
    private static final Logger log = Logger.getLogger(MqttService.class.getName());

    public static final int SOCKOPT_BACKLOG_DEFAULT = 50;
    public static final int MAXIMUM_MESSAGE_SIZE_DEFAULT = 1024 * 1024;
    public static final int DEFAULT_MQTT_PORT     = 1883;
    public static final int DEFAULT_MQTT_SSL_PORT = 8883;

    /**
     * We use this class to identify special return messages from the handle()
     * methods in the above handler types.  This is creative abuse of the type
     * system.
     */
    public static class MqttSentinelMessage extends MqttMessage {
        // Package visibility
        MqttSentinelMessage() {
            super(null);
        }
    }

    public static final MqttSentinelMessage DROP_CONNECTION = new MqttSentinelMessage();

    /**
     * MqttHandler as an inner class to reduce the amount of API leakage from
     * the Netty classes.
     */
    private class MqttHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            checkNotNull(ctx);
            checkNotNull(message);

            // Look up the context for this connection.  If one does not exist this
            // means that this is a new connection and we should allocate a context
            // object.
            Context context = null;
            synchronized (contextMap) {
                context = contextMap.get(ctx);
                if (context == null) {
                    context = new Context(ctx);
                    contextMap.put(ctx, context);
                }
            }

            MqttMessage msg = (MqttMessage) message;

            // If we get an oversize message the fixed header (and the other fields)
            // will be null.  So we just drop the connection.  We let the protocol
            // handler do this so that we can properly handle any session-related
            // cleanup.
            if (msg.fixedHeader() == null) {
                log.warning("Oversize message");
                disconnect(ctx);
                return;
            }

            MqttMessage response = null;
            MqttMessageType type = msg.fixedHeader().messageType();

            switch(type) {
                case CONNECT:
                    response = connectHandler.handle(context, (MqttConnectMessage) msg);
                    break;

                case DISCONNECT:
                    response = disconnectHandler.handle(context, msg);
                    break;

                case PUBLISH:
                    response = publishHandler.handle(context, (MqttPublishMessage) msg);
                    break;

                case SUBSCRIBE:
                    response = subscribeHandler.handle(context, (MqttSubscribeMessage) msg);
                    break;

                case UNSUBSCRIBE:
                    response = unsubscribeHandler.handle(context, (MqttUnsubscribeMessage) msg);
                    break;

                case PINGREQ:
                    response = pingHandler.handle(context, msg);
                    break;

                default:
                    log.log(Level.WARNING, "Unknown message type: " + type);
            }

            // If the return value is a subtype of the sentinel type we have to
            // investigate further.
            if (response instanceof MqttSentinelMessage) {

                if (response == DROP_CONNECTION) {
                    disconnect(ctx);
                    return;
                }

                throw new RuntimeException("Unknown sentinel message type");
            }

            if (response != null) {
                ctx.writeAndFlush(response);
            }
        }

        private void disconnect(ChannelHandlerContext ctx) {
            // We will not be needing this anymore.
            synchronized (contextMap) {
                contextMap.remove(ctx);
            }

            ctx.close().addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        log.info("Disconnect complete [future]");
                    }
                });
        }
    }


    /**
     * Builder class for MqttService.  Instances of this builder are reusable.
     */
    public static final class Builder {

        private static final Logger log = Logger.getLogger(Builder.class.getName());

        private int port = DEFAULT_MQTT_PORT;
        private int sslPort = DEFAULT_MQTT_SSL_PORT;
        private int maxMessageSize = MAXIMUM_MESSAGE_SIZE_DEFAULT;
        private int sockOptBacklog = SOCKOPT_BACKLOG_DEFAULT;

        private ConnectHandler connectHandler         = (ctx, msg) -> {log.info("no connect handler defined"); return null;};
        private DisconnectHandler disconnectHandler   = (ctx, msg) -> {log.info("no disconnect handler defined"); return null;};
        private PublishHandler publishHandler         = (ctx, msg) -> {log.info("no publish handler defined"); return null;};
        private SubscribeHandler subscribeHandler     = (ctx, msg) -> {log.info("no subscribe handler defined"); return null;};
        private UnsubscribeHandler unsubscribeHandler = (ctx, msg) -> {log.info("no unsubscribe handler defined"); return null;};
        private PingHandler pingHandler               = (ctx, msg) -> {log.info("no ping handler defined"); return null;};

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        public Builder sslPort(final int sslPort) {
            this.sslPort = sslPort;
            return this;
        }

        public Builder maxMessageSize(int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        public Builder sockOptBacklog(int sockOptBacklog) {
            this.sockOptBacklog = sockOptBacklog;
            return this;
        }

        public Builder connect(final ConnectHandler connectHandler) {
            this.connectHandler = connectHandler;
            return this;
        }

        public Builder disconnect(final DisconnectHandler disconnectHandler) {
            this.disconnectHandler = disconnectHandler;
            return this;
        }

        public Builder publish(final PublishHandler publishHandler) {
            this.publishHandler = publishHandler;
            return this;
        }

        public Builder subscribe(final SubscribeHandler subscribeHandler) {
            this.subscribeHandler = subscribeHandler;
            return this;
        }

        public Builder unsubscribe(final UnsubscribeHandler unsubscribeHandler) {
            this.unsubscribeHandler = unsubscribeHandler;
            return this;
        }

        public Builder ping(final PingHandler pingHandler) {
            this.pingHandler = pingHandler;
            return this;
        }

        public MqttService build() {
            return new MqttService(port,
                                   sslPort,
                                   maxMessageSize,
                                   sockOptBacklog,
                                   connectHandler,
                                   disconnectHandler,
                                   publishHandler,
                                   subscribeHandler,
                                   unsubscribeHandler,
                                   pingHandler);
        }
    }


    // Interface definitions for handlers
    //
    public interface ConnectHandler {
        public MqttMessage handle(Context context, MqttConnectMessage msg);
    }

    public interface DisconnectHandler {
        public MqttMessage handle(Context context, MqttMessage msg);
    }

    public interface PublishHandler {
        public MqttMessage handle(Context context, MqttPublishMessage msg);
    }

    public interface SubscribeHandler {
        public MqttMessage handle(Context context, MqttSubscribeMessage msg);
    }

    public interface UnsubscribeHandler {
        public MqttMessage handle(Context context, MqttUnsubscribeMessage msg);
    }

    public interface PingHandler {
        public MqttMessage handle(Context context, MqttMessage msg);
    }

    // The command handlers
    //
    private final ConnectHandler connectHandler;
    private final DisconnectHandler disconnectHandler;
    private final PublishHandler publishHandler;
    private final SubscribeHandler subscribeHandler;
    private final UnsubscribeHandler unsubscribeHandler;
    private final PingHandler pingHandler;

    // Server related stuff
    //
    private int port = 0;
    private int sslPort;
    private final int maxMessageSize;
    private final int sockOptBacklog;

    private Boolean started = false;
    private ChannelFuture channelFuture;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    // Map from network connection to the context object used in this service.
    private final Map<ChannelHandlerContext, Context> contextMap = new HashMap<>();

    public MqttService(final int port,
                       final int sslPort,
                       final int maxMessageSize,
                       final int sockOptBacklog,
                       final ConnectHandler connectHandler,
                       final DisconnectHandler disconnectHandler,
                       final PublishHandler publishHandler,
                       final SubscribeHandler subscribeHandler,
                       final UnsubscribeHandler unsubscribeHandler,
                       final PingHandler pingHandler) {
        this.port = port;
        this.sslPort = sslPort;
        this.maxMessageSize = maxMessageSize;
        this.sockOptBacklog = sockOptBacklog;
        this.connectHandler =  checkNotNull(connectHandler);
        this.disconnectHandler =  checkNotNull(disconnectHandler);
        this.publishHandler =  checkNotNull(publishHandler);
        this.subscribeHandler =  checkNotNull(subscribeHandler);
        this.unsubscribeHandler =  checkNotNull(unsubscribeHandler);
        this.pingHandler =  checkNotNull(pingHandler);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public MqttService start() throws Exception {
        synchronized(started) {
            checkState(!started);
            started = true;
        }

        channelFuture = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .localAddress(port)
            .option(ChannelOption.SO_BACKLOG, sockOptBacklog)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                        .addLast(new MqttDecoder(maxMessageSize))
                        .addLast(new MqttEncoder())
                        .addLast(new MqttHandler());
                    }
                })
            .bind().sync();

        // If the port we specified was 0 then the network code will allocate an
        // unused port.  Hence we have to look up the port anew.
        port = ((InetSocketAddress)channelFuture.channel().localAddress()).getPort();

        log.info("Started MQTT Service, listening on port " + port);

        return this;
    }

    public int port() {
        return port;
    }

    public void shutdown() throws Exception {
        synchronized(started) {
            checkState(started);
        }

        // Shut down executors with no quiet period and zero timeout.
        // Make sure you shut down EventLoopGroup before closing the
        // channel or the thing will hang, hang, hang.
        bossGroup.shutdownGracefully(0,0,TimeUnit.SECONDS).sync();
        workerGroup.shutdownGracefully(0,0,TimeUnit.SECONDS).sync();
        channelFuture.channel().closeFuture().sync();

        bossGroup.terminationFuture().sync();
        workerGroup.terminationFuture().sync();
        log.log(Level.INFO, "Terminated MQTT Server at port " + port);
    }
}
