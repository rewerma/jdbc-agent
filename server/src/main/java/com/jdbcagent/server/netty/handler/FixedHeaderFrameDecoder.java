package com.jdbcagent.server.netty.handler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;

/**
 * 解析对应的header信息
 *
 * @version 1.0.0
 */
public class FixedHeaderFrameDecoder extends ReplayingDecoder<VoidEnum> {

    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer,
            VoidEnum state) throws Exception {
        return buffer.readBytes(buffer.readInt());
    }
}
