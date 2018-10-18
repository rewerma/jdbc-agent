package com.jdbcagent.client.netty.handler;

import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.netty.NettyUtils;
import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JDBC-Agent client netty clientHandler
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientHandler extends SimpleChannelHandler {
    private JdbcAgentDataSource jdbcAgentDataSource;
    private AtomicBoolean connected;
    private ConcurrentHashMap<Long, NettyResponse> responseMap;

    public ClientHandler(JdbcAgentDataSource jdbcAgentDataSource, AtomicBoolean connected,
                         ConcurrentHashMap<Long, NettyResponse> responseMap) {
        this.jdbcAgentDataSource = jdbcAgentDataSource;
        this.connected = connected;
        this.responseMap = responseMap;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // 连接后认证
        String packet = "93" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) +
                "|AA" + "" +
                "|AD" + "" +
                "|XW" + jdbcAgentDataSource.getTimeout() +
                "|XR" + jdbcAgentDataSource.getTimeout() +
                "|XV1";

        NettyUtils.write(ctx.getChannel(), packet.getBytes(StandardCharsets.UTF_8), null);
        super.channelConnected(ctx, e);

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        byte[] body = buffer.readBytes(buffer.readableBytes()).array();

        if (!connected.get()) {
            //如果未连接说明是认证返回的数据包
            String ackPacket = new String(body, StandardCharsets.UTF_8);
            if (!ackPacket.startsWith("64")) {
                throw new RuntimeException("error ack from jdbc agent server ");
            }
            String[] packetItems = ackPacket.split("\\|");
            for (String packetItem : packetItems) {
                if (packetItem.startsWith("AS")) {
                    //设置序列化类型
                    SerializeUtil.serializeType = Packet.SerializeType.valueOf(packetItem.substring(2));
                }
            }
            //通过认证设置为已连接
            connected.set(true);
        } else {
            Packet packet = Packet.parse(body, SerializeUtil.serializeType);
            NettyResponse nettyRes = responseMap.get(packet.getId());
            if (nettyRes != null) {
                nettyRes.setPacket(packet);
                nettyRes.getLatch().countDown();
            }
        }

        super.messageReceived(ctx, e);
    }


    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        //如果连接断开了关闭释放客户端连接
        jdbcAgentDataSource.close();
    }
}
