package com.jdbcagent.core.util;

public class ZookeeperPathUtils {

    public static final String ZOOKEEPER_SEPARATOR = "/";

    public static final String JA_ROOT_NODE = ZOOKEEPER_SEPARATOR + "jdbc-agent";

    public static final String CATALOG_NODE = JA_ROOT_NODE + ZOOKEEPER_SEPARATOR + "{0}";

    public static final String RUNNING_NODE = "running";
}
