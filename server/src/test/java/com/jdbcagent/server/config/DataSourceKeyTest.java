package com.jdbcagent.server.config;

import com.jdbcagent.server.config.JdbcAgentConf.DataSourceKey;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DataSourceKeyTest {

    @Test
    public void pipeInUsernameDoesNotSelectAnotherDataSource() throws Exception {
        JdbcAgentConf conf = new JdbcAgentConf();
        JdbcAgentConf.JdbcAgent agent = new JdbcAgentConf.JdbcAgent();
        agent.setCatalog("sales");
        agent.setDataSources(Arrays.asList(
                dataSource("alice|bob", "secret-pass", "jdbc:h2:mem:key_alice_bob;DB_CLOSE_DELAY=-1"),
                dataSource("alice", "bob|secret-pass", "jdbc:h2:mem:key_alice;DB_CLOSE_DELAY=-1")));
        conf.setJdbcAgent(agent);
        conf.init();

        DataSource aliceBob = JdbcAgentConf.getDataSource("sales", "alice|bob", "secret-pass");
        DataSource alice = JdbcAgentConf.getDataSource("sales", "alice", "bob|secret-pass");
        Assert.assertNotNull(aliceBob);
        Assert.assertNotNull(alice);
        Assert.assertNotSame(aliceBob, alice);
        Assert.assertNull(JdbcAgentConf.getDataSource("sales", "alice|bob", "bob|secret-pass"));

        Connection first = aliceBob.getConnection();
        try {
            Statement statement = first.createStatement();
            statement.execute("create table marker(id int)");
            statement.close();
        } finally {
            first.close();
        }

        Connection second = alice.getConnection();
        try {
            ResultSet tables = second.getMetaData().getTables(null, null, "MARKER", null);
            Assert.assertFalse(tables.next());
            tables.close();
        } finally {
            second.close();
        }

        for (DataSourceKey key : JdbcAgentConf.dataSourceKeys()) {
            Assert.assertFalse(key.getCatalog().contains("secret-pass"));
            Assert.assertFalse(key.getUsername().contains("secret-pass"));
        }
    }

    private static DataSourceConf dataSource(String username, String password, String jdbcUrl) {
        DataSourceConf dataSourceConf = new DataSourceConf();
        dataSourceConf.setAccessUsername(username);
        dataSourceConf.setAccessPassword(password);
        Map<String, Object> dsManager = new HashMap<String, Object>();
        dsManager.put("driverClass", "org.h2.Driver");
        dsManager.put("jdbcUrl", jdbcUrl);
        dsManager.put("username", "sa");
        dsManager.put("password", "");
        dataSourceConf.setDsManager(dsManager);
        return dataSourceConf;
    }
}
