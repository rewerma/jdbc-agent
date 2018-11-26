package com.jdbcagent.test.server;


import com.jdbcagent.core.support.SerialRowSetImpl;

import javax.sql.RowSet;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 resultSet 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ResultSetServer {

    private ResultSet resultSet;                                    // 实际调用的resultSet

    private SerialRowSetImpl serialRowSet;                          // 可序列化的rowSet

    /**
     * 构造方法
     *
     * @param resultSet
     * @throws SQLException
     */
    public ResultSetServer(ResultSet resultSet) throws SQLException {
        try {
            this.resultSet = resultSet;
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
