package com.jdbcagent.client.netty;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.client.netty.handler.ClientHandler;
import com.jdbcagent.client.netty.handler.FixedHeaderFrameDecoder;
import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JDBC-Agent client netty 客户端
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentNettyClient extends JdbcAgentConnector {
    private AtomicBoolean connected = new AtomicBoolean(false);          //是否已经连接

    private volatile boolean running = false;                            // 是否运行中

    private String ip;                                                   // 启动IP

    private int port;                                                    // 启动端口

    private ClientBootstrap bootstrap = null;

    private Channel channel;

    private JdbcAgentDataSource jdbcAgentDataSource;

    public ConcurrentHashMap<Long, NettyResponse> responseMap;

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public JdbcAgentNettyClient(JdbcAgentDataSource jdbcAgentDataSource) {
        this.jdbcAgentDataSource = jdbcAgentDataSource;
        responseMap = new ConcurrentHashMap<>();
    }

    /**
     * 客户端启动
     */
    public void start() {
        if (running) {
            throw new RuntimeException(this.getClass().getName() + " has startup , don't repeat start");
        }
        running = true;

        try {
            bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));
            bootstrap.setOption("child.keepAlive", true);
            bootstrap.setOption("child.tcpNoDelay", true);
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                @Override
                public ChannelPipeline getPipeline() throws Exception {
                    ChannelPipeline pipeline = Channels.pipeline();
                    pipeline.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
                    pipeline.addLast(ClientHandler.class.getName(), new ClientHandler(jdbcAgentDataSource, connected, responseMap));
                    return pipeline;
                }
            });
            ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(ip, port)).sync();
            channel = channelFuture.getChannel();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Channel getChannel() {
        return channel;
    }

    /**
     * 写数据
     *
     * @param packet 数据包
     * @return
     * @throws SQLException
     */
    public byte[] write(Packet packet) throws SQLException {
        while (!connected.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                //ignore
            }
        }

        NettyResponse nettyRes = new NettyResponse();
        CountDownLatch latch = new CountDownLatch(1);
        nettyRes.setLatch(latch);

        responseMap.put(packet.getId(), nettyRes);
        NettyUtils.write(getChannel(), packet, null);

        try {
            latch.await();
            Packet packetAck = responseMap.remove(packet.getId()).getPacket();
            return packetAck.toByteArray(SerializeUtil.serializeType);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * 停止客户端
     */
    public void stop() {
        connected.set(false);

        if (!running) {
            return;
            // throw new RuntimeException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;

        if (this.channel != null) {
            channel.close();
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }
    }
}
