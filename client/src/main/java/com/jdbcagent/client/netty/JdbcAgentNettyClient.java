package com.jdbcagent.client.netty;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.client.netty.handler.ClientHandler;
import com.jdbcagent.client.uitl.ZookeeperUtil;
import com.jdbcagent.client.netty.handler.FixedHeaderFrameDecoder;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.util.TransportSecurity;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JDBC-Agent client netty 客户端
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentNettyClient implements JdbcAgentConnector {
    public static volatile boolean connected = false;   //是否已经连接

    public static volatile String authError;            // 握手失败原因

    private volatile boolean running = false;           // 是否运行中

    private String ip;                                  // 启动IP

    private int port;                                    // 启动端口

    private Bootstrap bootstrap = null;

    private EventLoopGroup group;

    private Channel channel;

    private SSLContext sslContext;

    private JdbcAgentDataSource jdbcAgentDataSource;

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public JdbcAgentNettyClient(JdbcAgentDataSource jdbcAgentDataSource) {
        this.jdbcAgentDataSource = jdbcAgentDataSource;
    }

    /**
     * 客户端启动
     */
    public void start() {
        if (running) {
            throw new RuntimeException(this.getClass().getName() + " has startup , don't repeat start");
        }
        if (sslContext == null) {
            throw new IllegalStateException("sslTrustStore is required");
        }
        running = true;
        connected = false;
        authError = null;

        try {
            group = new NioEventLoopGroup(1, namedThreads("jdbc-agent-client-io-"));
            bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast("tls",
                                    new SslHandler(TransportSecurity.clientEngine(sslContext, ip, port)));
                            ch.pipeline().addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
                            ch.pipeline().addLast(ClientHandler.class.getName(), new ClientHandler(jdbcAgentDataSource));
                        }
                    });
            ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(ip, port)).sync();
            channel = channelFuture.channel();
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
        awaitConnected();
        if (packet.getId() == null) {
            throw new SQLException("packet id is null");
        }

        NettyResponse nettyRes = new NettyResponse();
        ReentrantLock lock = NettyUtils.lock;
        lock.lock();
        try {
            nettyRes.setCondition(lock.newCondition());
            NettyUtils.RESPONSE_MAP.put(packet.getId(), nettyRes);
        } finally {
            lock.unlock();
        }

        try {
            NettyUtils.write(getChannel(), packet, null);
            lock.lock();
            try {
                return NettyUtils.awaitResponse(nettyRes, responseTimeoutMillis()).toByteArray();
            } finally {
                lock.unlock();
            }
        } catch (SQLException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while waiting for response", e);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            NettyUtils.RESPONSE_MAP.remove(packet.getId());
        }
    }

    private void awaitConnected() throws SQLException {
        NettyUtils.lock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(responseTimeoutMillis());
            while (!connected) {
                if (authError != null) {
                    throw new SQLException(authError);
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new SQLException("timeout waiting for authentication");
                }
                NettyUtils.CONNECTED.await(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while waiting for authentication", e);
        } finally {
            NettyUtils.lock.unlock();
        }
    }

    private long responseTimeoutMillis() {
        int timeout = jdbcAgentDataSource.getTimeout();
        return timeout > 0 ? timeout : 60000L;
    }

    /**
     * 停止客户端
     */
    public void stop() {
        connected = false;
        NettyUtils.failPending("connection closed");

        if (!running) {
            releaseBootstrap();
            return;
            // throw new RuntimeException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;

        Channel previous = this.channel;
        this.channel = null;
        if (previous != null) {
            previous.close();
        }

        releaseBootstrap();
    }

    /**
     * 断开当前通道并按已设置的 ip/port 重新连接。调用前先把 channel 置空，避免旧通道的关闭回调关掉 DataSource。
     */
    public void reconnect() {
        Channel previous = this.channel;
        this.channel = null;
        if (previous != null) {
            previous.close();
        }
        releaseBootstrap();
        running = false;
        connected = false;
        authError = null;

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ZookeeperUtil.FAILOVER_WAIT_MILLIS);
        RuntimeException last = null;
        while (true) {
            try {
                start();
                return;
            } catch (RuntimeException e) {
                last = e;
                running = false;
                connected = false;
                this.channel = null;
                releaseBootstrap();
                if (System.nanoTime() >= deadline) {
                    throw last;
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw last;
                }
            }
        }
    }

    private void releaseBootstrap() {
        if (group != null) {
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
            group = null;
        }
        bootstrap = null;
    }

    public void connect() {

    }

    public void disconnect() {

    }

    private static ThreadFactory namedThreads(final String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, prefix + index.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        };
    }
}
