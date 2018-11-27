package com.jdbcagent.server;

import com.jdbcagent.server.config.Configuration;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.netty.JdbcAgentNettyServer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * JDBC-Agent server 启动类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ServerLauncher {
    private static Logger logger = LoggerFactory.getLogger(ServerLauncher.class);

    public static void main(String[] args) {
        try {
            logger.info("## load jdbc-agent configurations");
            String CLASSPATH_URL_PREFIX = "classpath:";
            String conf2 = System.getProperty("ja.conf", "classpath:config.properties");

            JdbcAgentConf jdbcAgentConf;
            try {
                InputStream in;
                if (conf2.startsWith(CLASSPATH_URL_PREFIX)) {
                    conf2 = StringUtils.substringAfter(conf2, CLASSPATH_URL_PREFIX);
                    in = ServerLauncher.class.getClassLoader().getResourceAsStream(conf2);
                } else {
                    in = new FileInputStream(conf2);
                }

                jdbcAgentConf = Configuration.loadConf(in);
                in.close();

                Configuration.loadDS();
                jdbcAgentConf.init();
            } catch (Exception e) {
                throw new RuntimeException("## failed to load the config file: jdbc-agent.yml");
            }

            logger.info("## start the jdbc-agent server");
            logger.info("## serialize type: " + Configuration.getJdbcAgentCon().getJdbcAgent().getSerialize());
            final JdbcAgentNettyServer server = JdbcAgentNettyServer.instance();
            server.setJdbcAgentConf(jdbcAgentConf);
            server.start();
            logger.info("## the jdbc-agent server is running now ......");

            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the jdbc-agent server");
                        server.stop();
                    } catch (Throwable e) {
                        logger.warn("## something goes wrong when stopping jdbc-agent server:", e);
                    } finally {
                        logger.info("## jdbc-agent server is down.");
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("## something goes wrong when starting up the jdbc agent server:", e);
            System.exit(0);
        }
    }
}
