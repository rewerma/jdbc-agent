package com.jdbcagent.server.jdbc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.jdbcagent.core.support.SerialRowSetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.RowSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC-Agent server 端 resultSet 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ResultSetServer {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetServer.class);

    private static AtomicLong RESULTSET_ID = new AtomicLong(0);     // id与client对应

    private static final ConcurrentHashMap<Integer, Set<Long>> CHANNEL_RESULTSETS =
            new ConcurrentHashMap<Integer, Set<Long>>();

    static final RemovalListener<Long, ResultSetServer> CLOSE_ON_REMOVAL =
            new RemovalListener<Long, ResultSetServer>() {
                @Override
                public void onRemoval(RemovalNotification<Long, ResultSetServer> notification) {
                    ResultSetServer resultSetServer = notification.getValue();
                    if (resultSetServer != null) {
                        resultSetServer.closeQuietly();
                    }
                }
            };

    public static Cache<Long, ResultSetServer> RESULTSETS =         // resultSet 缓存, 60分钟未访问后关闭并移除
            CacheBuilder.newBuilder()
                    .initialCapacity(50)
                    .concurrencyLevel(100)
                    .expireAfterAccess(60, TimeUnit.MINUTES)
                    .removalListener(CLOSE_ON_REMOVAL)
                    .build();

    static {
        ServerCacheMaintenance.start();
    }

    long currentId;                                                 // 当前id

    private ResultSet resultSet;                                    // 实际调用的resultSet

    private SerialRowSetImpl serialRowSet;                          // 可序列化的rowSet

    private final Integer channelId;

    private final AtomicBoolean lobsReleased = new AtomicBoolean(false);

    /**
     * 构造方法
     *
     * @param resultSet
     * @throws SQLException
     */
    ResultSetServer(ResultSet resultSet) throws SQLException {
        try {
            currentId = RESULTSET_ID.incrementAndGet();
            this.resultSet = resultSet;
            this.channelId = DispatchChannels.get();
            bindChannel();
            RESULTSETS.put(currentId, this);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * 关闭方法
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        try {
            closeResultSet();
            RESULTSETS.invalidate(currentId);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private void closeResultSet() throws SQLException {
        ResultSet toClose;
        synchronized (this) {
            toClose = resultSet;
            resultSet = null;
        }
        if (toClose == null) {
            releaseLobs();
            return;
        }
        try {
            if (!toClose.isClosed()) {
                toClose.close();
            }
            releaseLobs();
        } catch (SQLException e) {
            synchronized (this) {
                if (resultSet == null) {
                    resultSet = toClose;
                }
            }
            throw e;
        }
    }

    /**
     * 通道断开时关闭该通道上的结果集，并删除还没读完的大字段文件。
     */
    public static void closeChannel(Integer channelId) {
        if (channelId == null) {
            return;
        }
        Set<Long> ids = CHANNEL_RESULTSETS.remove(channelId);
        if (ids == null) {
            return;
        }
        Long[] snapshot;
        synchronized (ids) {
            snapshot = ids.toArray(new Long[ids.size()]);
            ids.clear();
        }
        for (Long id : snapshot) {
            ResultSetServer resultSetServer = RESULTSETS.getIfPresent(id);
            if (resultSetServer == null) {
                LobStore.deleteByResultSet(id);
                continue;
            }
            try {
                resultSetServer.close();
            } catch (SQLException e) {
                logger.warn("close resultSet failed, id=" + id, e);
                resultSetServer.releaseLobs();
            }
        }
    }

    private void bindChannel() {
        if (channelId == null) {
            return;
        }
        Set<Long> created = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        Set<Long> existing = CHANNEL_RESULTSETS.putIfAbsent(channelId, created);
        Set<Long> ids = existing == null ? created : existing;
        synchronized (ids) {
            ids.add(currentId);
        }
    }

    private void unbindChannel() {
        if (channelId == null) {
            return;
        }
        Set<Long> ids = CHANNEL_RESULTSETS.get(channelId);
        if (ids == null) {
            return;
        }
        synchronized (ids) {
            ids.remove(currentId);
            if (ids.isEmpty()) {
                CHANNEL_RESULTSETS.remove(channelId, ids);
            }
        }
    }

    private void releaseLobs() {
        if (!lobsReleased.compareAndSet(false, true)) {
            return;
        }
        LobStore.deleteByResultSet(currentId);
        unbindChannel();
    }

    private void closeQuietly() {
        try {
            closeResultSet();
        } catch (Exception e) {
            logger.warn("close resultSet failed, id=" + currentId, e);
        }
    }

    /**
     * 获取rs的元数据
     *
     * @return RowSet
     * @throws SQLException
     */
    public RowSet getMetaData() throws SQLException {
        if (resultSet != null) {
            if (serialRowSet == null) {
                serialRowSet = new SerialRowSetImpl(resultSet);
            }

            serialRowSet.populateRSMD();

            return serialRowSet;
        }
        return null;
    }

    /**
     * 获取指定数量的rs记录集合转为可序列化的RowSet
     *
     * @param size 批大小
     * @return RowSet
     * @throws SQLException
     */
    public RowSet fetchRows(int size) throws SQLException {
        if (resultSet != null) {
            if (serialRowSet == null) {
                serialRowSet = new SerialRowSetImpl(resultSet);
            }
            serialRowSet.setLobMaterializer(new ServerLobMaterializer(currentId));
            serialRowSet.populate(size);

            return serialRowSet;
        }
        return null;
    }
}
