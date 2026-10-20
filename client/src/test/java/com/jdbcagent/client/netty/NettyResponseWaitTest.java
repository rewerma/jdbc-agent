package com.jdbcagent.client.netty;

import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.core.protocol.Packet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class NettyResponseWaitTest {

    @Test
    public void returnsPacketSignaledBeforeWait() throws Exception {
        NettyResponse response = newResponse();
        Packet packet = Packet.newBuilder().setType(Packet.PacketType.ACK).build();
        NettyUtils.lock.lock();
        try {
            response.setPacket(packet);
            response.getCondition().signal();
            Assert.assertSame(packet, NettyUtils.awaitResponse(response, 200));
        } finally {
            NettyUtils.lock.unlock();
        }
    }

    @Test
    public void timesOutWhenNoResponseArrives() throws Exception {
        NettyResponse response = newResponse();
        NettyUtils.lock.lock();
        try {
            NettyUtils.awaitResponse(response, 50);
            Assert.fail("wait should time out");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        } finally {
            NettyUtils.lock.unlock();
        }
    }

    @Test
    public void wakesWhenResponseIsSignaled() throws Exception {
        final NettyResponse response = newResponse();
        final Packet packet = Packet.newBuilder().setType(Packet.PacketType.ACK).build();
        Thread responder = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                NettyUtils.lock.lock();
                try {
                    response.setPacket(packet);
                    response.getCondition().signal();
                } finally {
                    NettyUtils.lock.unlock();
                }
            }
        });
        responder.start();

        NettyUtils.lock.lock();
        try {
            Assert.assertSame(packet, NettyUtils.awaitResponse(response, 2000));
        } finally {
            NettyUtils.lock.unlock();
        }
        responder.join(2000);
    }

    private static NettyResponse newResponse() {
        NettyUtils.lock.lock();
        try {
            NettyResponse response = new NettyResponse();
            response.setCondition(NettyUtils.lock.newCondition());
            return response;
        } finally {
            NettyUtils.lock.unlock();
        }
    }
}
