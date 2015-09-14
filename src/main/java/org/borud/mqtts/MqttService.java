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
import java.util.concurrent.TimeUnit;
import static com.google.common.base.Preconditions.*;

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
public final class MqttService extends ChannelInboundHandlerAdapter {
    private static final Logger log = Logger.getLogger(MqttService.class.getName());

    public static final int SOCKOPT_BACKLOG_DEFAULT = 50;
    public static final int MAXIMUM_MESSAGE_SIZE_DEFAULT = 1024 * 1024;

    // The command handlers
    //
    private final ConnectHandler connectHandler;
    private final DisconnectHandler disconnectHandler;
    private final PublishHandler publishHandler;
    private final SubscribeHandler subscribeHandler;
    private final UnsubscribeHandler unsubscribeHandler;
    private final PingHandler pingHandler;

    // Netty server related stuff
    //
    private int port;
    private ChannelFuture channelFuture;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();


    // Map from network connection to the context object used in this service.
    private final Map<ChannelHandlerContext, Context> contextMap = new HashMap<>();

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

    /**
     * Builder class for MqttService.  Instances of this builder are reusable.
     */
    public static final class Builder {
        private int port = 0;
        private ConnectHandler connectHandler;
        private DisconnectHandler disconnectHandler;
        private PublishHandler publishHandler;
        private SubscribeHandler subscribeHandler;
        private UnsubscribeHandler unsubscribeHandler;
        private PingHandler pingHandler;

        public Builder port(final int port) {
            this.port = port;
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
                                   connectHandler,
                                   disconnectHandler,
                                   publishHandler,
                                   subscribeHandler,
                                   unsubscribeHandler,
                                   pingHandler);
        }
    }

    public MqttService(final int port,
                       final ConnectHandler connectHandler,
                       final DisconnectHandler disconnectHandler,
                       final PublishHandler publishHandler,
                       final SubscribeHandler subscribeHandler,
                       final UnsubscribeHandler unsubscribeHandler,
                       final PingHandler pingHandler) {
        this.port = port;
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


        if (response != null) {
            ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        log.info("Response sent.");
                    }
                });
        }
    }

    public void start() throws Exception {
        channelFuture = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .localAddress(port)
            .option(ChannelOption.SO_BACKLOG, SOCKOPT_BACKLOG_DEFAULT)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                        .addLast(new MqttDecoder(MAXIMUM_MESSAGE_SIZE_DEFAULT))
                        .addLast(new MqttEncoder())
                        .addLast(this);
                    }
                })
            .bind().sync();
    }

}
