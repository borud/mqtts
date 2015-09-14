package org.borud.mqtts;

import io.netty.channel.ChannelHandlerContext;

/**
 * Context associated with the network connection.
 *
 * @author borud
 */
public class Context {
    private final ChannelHandlerContext channelHandlerContext;

    private String clientIdentifier;
    private String username;
    private boolean authenticated;

    public Context(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    public String clientIdentifier() {
        return clientIdentifier;
    }

    public Context clientIdentifier(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
        return this;
    }

    public String username() {
        return username;
    }

    public Context username(String username) {
        this.username = username;
        return this;
    }
}
