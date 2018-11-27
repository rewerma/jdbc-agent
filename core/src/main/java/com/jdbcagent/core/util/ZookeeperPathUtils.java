package com.jdbcagent.core.util;

/**
 * JDBC-Agent zookeeper路径常量类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ZookeeperPathUtils {

    public static final String ZOOKEEPER_SEPARATOR = "/";

    public static final String JA_ROOT_NODE = ZOOKEEPER_SEPARATOR + "jdbc-agent";

    public static final String SERVER_NODE = ZOOKEEPER_SEPARATOR + "server";

    public static final String CATALOG_NODE = JA_ROOT_NODE + SERVER_NODE + ZOOKEEPER_SEPARATOR + "{0}";

    public static final String RUNNING_NODE = "running";
}
