package com.jdbcagent.server.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jdbcagent.core.support.SerialRowSetImpl;

import javax.sql.RowSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC-Agent server 端 resultSet 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ResultSetServer {
    private static AtomicLong RESULTSET_ID = new AtomicLong(0);     // id与client对应

    public static Cache<Long, ResultSetServer> RESULTSETS = Caffeine.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .maximumSize(100000)
            .build();                                               // resultSetServer 缓存, 保存60分钟自动删除

    long currentId;                                                 // 当前id

    private ResultSet resultSet;                                    // 实际调用的resultSet

    private SerialRowSetImpl serialRowSet;                          // 可序列化的rowSet

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
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
                resultSet = null;
            }
            RESULTSETS.invalidate(currentId);
        } catch (Exception e) {
            throw new SQLException(e);
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
            // 装填RowSet
            serialRowSet.populate(size);

            return serialRowSet;
        }
        return null;
    }
}
