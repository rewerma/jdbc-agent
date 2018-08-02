package com.jdbcagent.server.example;

import com.jdbcagent.server.config.ConfigParser;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.netty.JdbcAgentNettyServer;
import com.jdbcagent.server.test.JdbcAgentServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class JdbcAgentServerExample {
    private static Logger logger = LoggerFactory.getLogger(JdbcAgentServerExample.class);

    public static void main(String[] args) {
        try {
            final JdbcAgentNettyServer jdbcAgentServer = JdbcAgentNettyServer.instance();
            InputStream in = JdbcAgentServerExample.class.getClassLoader().getResourceAsStream("jdbc-agent.yml");
            JdbcAgentConf jdbcAgentConf = ConfigParser.parse(in);
//            jdbcAgentConf.init();
            jdbcAgentServer.setJdbcAgentConf(jdbcAgentConf);
            jdbcAgentServer.start();

            JdbcAgentServerTest.ddl();
            logger.info("jdbc agent server started");

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    jdbcAgentServer.stop();
                    logger.info("jdbc agent server stopped");
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
