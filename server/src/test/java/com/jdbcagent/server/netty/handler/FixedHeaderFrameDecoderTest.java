package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.Packet;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

public class FixedHeaderFrameDecoderTest {

    @Test
    public void rejectsLengthAboveLimit() {
        EmbeddedChannel channel = new EmbeddedChannel(new FixedHeaderFrameDecoder());
        try {
            channel.writeInbound(Unpooled.buffer().writeInt(Packet.MAX_BODY_LENGTH + 1));
            Assert.fail("oversized frame should be rejected");
        } catch (Exception e) {
            Assert.assertTrue(rootMessage(e).contains("Illegal frame length"));
        } finally {
            channel.close();
        }
    }

    @Test
    public void rejectsNegativeLength() {
        EmbeddedChannel channel = new EmbeddedChannel(new FixedHeaderFrameDecoder());
        try {
            channel.writeInbound(Unpooled.buffer().writeInt(-1));
            Assert.fail("negative frame length should be rejected");
        } catch (Exception e) {
            Assert.assertTrue(rootMessage(e).contains("Illegal frame length"));
        } finally {
            channel.close();
        }
    }

    @Test
    public void readsFrameWithinLimit() {
        EmbeddedChannel channel = new EmbeddedChannel(new FixedHeaderFrameDecoder());
        Assert.assertTrue(channel.writeInbound(Unpooled.buffer().writeInt(4).writeBytes(new byte[]{1, 2, 3, 4})));
        byte[] frame = (byte[]) channel.readInbound();
        Assert.assertEquals(4, frame.length);
        Assert.assertEquals(1, frame[0]);
        channel.close();
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? "" : current.getMessage();
    }
}
