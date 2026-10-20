package com.jdbcagent.core.protocol;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 在把一行写入 RowSet 之前处理大字段列。
 */
public interface LobMaterializer {
    /**
     * 返回该对象表示这一列仍走原来的 {@code getObject} 路径。
     */
    Object PASSTHROUGH = new Object();

    /**
     * @return {@link #PASSTHROUGH}，或要放进单元格的值（包含 SQL NULL）
     */
    Object materialize(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException;
}
