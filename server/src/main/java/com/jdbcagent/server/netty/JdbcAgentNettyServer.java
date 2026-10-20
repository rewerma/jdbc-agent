package com.jdbcagent.server.netty;

import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.core.util.TransportSecurity;
import com.jdbcagent.server.JdbcAgentServer;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.netty.handler.ClientAuthenticationHandler;
import com.jdbcagent.server.netty.handler.FixedHeaderFrameDecoder;
import com.jdbcagent.server.netty.handler.SessionHandler;
import com.jdbcagent.server.running.ServerRunningData;
import com.jdbcagent.server.running.ServerRunningListener;
import com.jdbcagent.server.running.ServerRunningMonitor;
import com.jdbcagent.server.util.AddressUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.I0Itec.zkclient.ZkClient;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent server netty server
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentNettyServer implements JdbcAgentServer {
    private volatile boolean running = false;   // 是否运行中

    private JdbcAgentConf jdbcAgentConf;        // 配置项

    private Channel serverChannel = null;

    private ServerBootstrap bootstrap = null;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private volatile SSLContext sslContext;

    private ChannelGroup childGroups;           // socket channel container, used to close sockets explicitly.

    private ServerRunningMonitor runningMonitor;

    public void setJdbcAgentConf(JdbcAgentConf jdbcAgentConf) {
        this.jdbcAgentConf = jdbcAgentConf;
    }

    public JdbcAgentNettyServer() {
        this.childGroups = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    }

    private static class SingletonHolder {
        private static final JdbcAgentNettyServer JDBC_AGENT_NETTY_SERVER = new JdbcAgentNettyServer();
    }

    public static JdbcAgentNettyServer instance() {
        return SingletonHolder.JDBC_AGENT_NETTY_SERVER;
    }

    private void initZk() {
        if (jdbcAgentConf.getJdbcAgent().getZkServers() != null && runningMonitor == null) {
            ZkClient zkClient = new ZkClient(jdbcAgentConf.getJdbcAgent().getZkServers()
                    , 3000, 3000, new ByteSerializer());

            ServerRunningData serverData = new ServerRunningData();
            serverData.setCatalog(jdbcAgentConf.getJdbcAgent().getCatalog());
            String ip = jdbcAgentConf.getJdbcAgent().getIp();
            if (ip == null) {
                ip = AddressUtils.getHostIp();
            }
            serverData.setAddress(ip + ":" + jdbcAgentConf.getJdbcAgent().getPort());

            runningMonitor = new ServerRunningMonitor();
            runningMonitor.setJdbcAgentConf(jdbcAgentConf);
            runningMonitor.setZkClient(zkClient);
            runningMonitor.setServerData(serverData);
        }
    }

    /**
     * 启动netty server
     */
    @Override
    public void start() {
        if (running) {
            throw new RuntimeException(
                    this.getClass().getName() + " has startup , don't repeat start");
        }

        initZk();

        try {
            JdbcAgentConf.JdbcAgent agent = jdbcAgentConf.getJdbcAgent();
            sslContext = TransportSecurity.serverContext(
                    agent.getSslKeyStore(), agent.getSslKeyStorePassword(), agent.getSslKeyStoreType());
            createBootstrap();
            if (runningMonitor != null) {
                if (!runningMonitor.isStart()) {
                    runningMonitor.setListener(new ServerRunningListener() {
                        @Override
                        public void processActiveEnter() {
                            bind();
                        }

                        @Override
                        public void processActiveExit() {
                            unbind();
                        }
                    });
                    runningMonitor.start();
                    runningMonitor.waitForActive();
                    bind();
                }
            } else {
                bind();
            }
            running = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            unbind();
            JdbcDispatch.shutdown();
            releaseBootstrap();
            throw new RuntimeException("interrupted while waiting for active", e);
        } catch (RuntimeException e) {
            unbind();
            JdbcDispatch.shutdown();
            releaseBootstrap();
            throw e;
        }
    }

    private void createBootstrap() {
        int ioThreads = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);
        bossGroup = new NioEventLoopGroup(1, JdbcDispatch.namedThreads("jdbc-agent-boss-"));
        workerGroup = new NioEventLoopGroup(ioThreads, JdbcDispatch.namedThreads("jdbc-agent-io-"));
        this.bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        childGroups.add(channel);
                        channel.pipeline().addLast("tls", new SslHandler(TransportSecurity.serverEngine(sslContext)));
                        channel.pipeline().addLast("frame", new FixedHeaderFrameDecoder());
                        channel.pipeline().addLast(ClientAuthenticationHandler.class.getName(),
                                new ClientAuthenticationHandler());
                        channel.pipeline().addLast(SessionHandler.class.getName(), new SessionHandler());
                    }
                });
    }

    private void bind() {
        if (serverChannel != null && serverChannel.isActive()) {
            return;
        }
        String ip = jdbcAgentConf.getJdbcAgent().getIp();
        int port = jdbcAgentConf.getJdbcAgent().getPort();
        if (ip != null) {
            serverChannel = bootstrap.bind(new InetSocketAddress(ip, port)).syncUninterruptibly().channel();
        } else {
            serverChannel = bootstrap.bind(new InetSocketAddress(port)).syncUninterruptibly().channel();
        }
        childGroups.add(serverChannel);
    }

    private void unbind() {
        if (childGroups != null) {
            childGroups.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
        serverChannel = null;
    }

    private void releaseBootstrap() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(2, TimeUnit.SECONDS);
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(2, TimeUnit.SECONDS);
            workerGroup = null;
        }
        bootstrap = null;
        sslContext = null;
    }

    /**
     * 停止netty server
     */
    @Override
    public void stop() {
        if (!running) {
            throw new RuntimeException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;

        if (runningMonitor != null && runningMonitor.isStart()) {
            runningMonitor.stop();
        }

        // 失去 active 或进程退出时关掉监听端口和已接入的连接，客户端才会切到新主机
        unbind();
        JdbcDispatch.shutdown();
        releaseBootstrap();
    }

    @Override
    public boolean isStart() {
        return running;
    }
}
