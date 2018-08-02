package com.jdbcagent.server.netty;

import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.server.JdbcAgentServer;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.netty.handler.ClientAuthenticationHandler;
import com.jdbcagent.server.netty.handler.FixedHeaderFrameDecoder;
import com.jdbcagent.server.netty.handler.SessionHandler;
import com.jdbcagent.server.running.ServerRunningData;
import com.jdbcagent.server.running.ServerRunningListener;
import com.jdbcagent.server.running.ServerRunningMonitor;
import com.jdbcagent.server.util.AddressUtils;
import org.I0Itec.zkclient.ZkClient;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

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

    private ChannelGroup childGroups;           // socket channel container, used to close sockets explicitly.

    private List<ServerRunningMonitor> runningMonitors;

    public void setJdbcAgentConf(JdbcAgentConf jdbcAgentConf) {
        this.jdbcAgentConf = jdbcAgentConf;
    }

    public JdbcAgentNettyServer() {
        this.childGroups = new DefaultChannelGroup();
    }

    private static class SingletonHolder {
        private static final JdbcAgentNettyServer JDBC_AGENT_NETTY_SERVER = new JdbcAgentNettyServer();
    }

    public static JdbcAgentNettyServer instance() {
        return SingletonHolder.JDBC_AGENT_NETTY_SERVER;
    }

    private void initZk() {
        if (jdbcAgentConf.getJdbcAgent().getZkServers() != null && runningMonitors == null) {
            ZkClient zkClient = new ZkClient(jdbcAgentConf.getJdbcAgent().getZkServers()
                    , 3000, 3000, new ByteSerializer());

            runningMonitors = new ArrayList<>();

            for (JdbcAgentConf.Catalog catalog : jdbcAgentConf.getJdbcAgent().getCatalogs()) {
                ServerRunningData serverData = new ServerRunningData();
                serverData.setCatalog(catalog.getCatalog());
                String ip = jdbcAgentConf.getJdbcAgent().getIp();
                if (ip == null) {
                    ip = AddressUtils.getHostIp();
                }
                serverData.setAddress(ip + ":" + jdbcAgentConf.getJdbcAgent().getPort());

                final ServerRunningMonitor runningMonitor = new ServerRunningMonitor();
                runningMonitor.setJdbcAgentConf(jdbcAgentConf);
                runningMonitor.setCatalog(catalog);
                runningMonitor.setZkClient(zkClient);
                runningMonitor.setServerData(serverData);
                runningMonitor.setListener(new ServerRunningListener() {
                    @Override
                    public void processActiveEnter() {
                        jdbcAgentConf.initDataSource(runningMonitor.getCatalog());
                    }

                    @Override
                    public void processActiveExit() {
                        jdbcAgentConf.closeDataSource(runningMonitor.getCatalog());
                    }
                });
                runningMonitors.add(runningMonitor);
            }
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

        if (runningMonitors != null) {
            for (ServerRunningMonitor runningMonitor : runningMonitors) {
                if (!runningMonitor.isStart()) {
                    runningMonitor.start();
                    if (runningMonitor.check()) {
                        jdbcAgentConf.initDataSource(runningMonitor.getCatalog());
                    }
                }
            }
        } else {
            // 初始化所有数据源
            jdbcAgentConf.initAllDS();
        }

        running = true;

        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        /*
         * enable keep-alive mechanism, handle abnormal network connection scenarios on OS level.
         * the threshold parameters are depended on OS. e.g. On Linux: net.ipv4.tcp_keepalive_time =
         * 300 net.ipv4.tcp_keepalive_probes = 2 net.ipv4.tcp_keepalive_intvl = 30
         */
        bootstrap.setOption("child.keepAlive", true);
        /*
         * optional parameter.
         */
        bootstrap.setOption("child.tcpNoDelay", true);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast(FixedHeaderFrameDecoder.class.getName(),
                        new FixedHeaderFrameDecoder());
                pipeline.addLast(ClientAuthenticationHandler.class.getName(), new ClientAuthenticationHandler());
                pipeline.addLast(SessionHandler.class.getName(), new SessionHandler());
                return pipeline;
            }

        });

        if (jdbcAgentConf.getJdbcAgent().getIp() != null) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(jdbcAgentConf.getJdbcAgent().getIp(), jdbcAgentConf.getJdbcAgent().getPort()));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(jdbcAgentConf.getJdbcAgent().getPort()));
        }
        this.childGroups.add(serverChannel);
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

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        jdbcAgentConf.closeAllDS();

        if (runningMonitors != null) {
            for (ServerRunningMonitor runningMonitor : runningMonitors) {
                if (runningMonitor != null && runningMonitor.isStart()) {
                    runningMonitor.stop();
                }
            }
        }
    }

    @Override
    public boolean isStart() {
        return running;
    }
}
