package com.jdbcagent.server;

import com.jdbcagent.server.config.ConfigParser;
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
            String conf = System.getProperty("ja.conf", "classpath:jdbc-agent.yml");
            InputStream in;
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                in = ServerLauncher.class.getClassLoader().getResourceAsStream(conf);
            } else {
                in = new FileInputStream(conf);
            }
            JdbcAgentConf jdbcAgentConf = ConfigParser.parse(in);
            in.close();
            jdbcAgentConf.init();

            logger.info("## start the jdbc-agent server");
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
