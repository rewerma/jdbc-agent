package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 解析对应的header信息
 *
 * @version 1.0.0
 */
public class FixedHeaderFrameDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(FixedHeaderFrameDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        if (buffer.readableBytes() < 4) {
            return;
        }
        buffer.markReaderIndex();
        int length = buffer.readInt();
        if (length < 0 || length > Packet.MAX_BODY_LENGTH) {
            logger.warn("reject frame, length={}, max={}", length, Packet.MAX_BODY_LENGTH);
            if (ctx != null) {
                ctx.close();
            }
            Packet.checkBodyLength(length);
        }
        if (buffer.readableBytes() < length) {
            buffer.resetReaderIndex();
            return;
        }
        byte[] body = new byte[length];
        buffer.readBytes(body);
        out.add(body);
    }
}
