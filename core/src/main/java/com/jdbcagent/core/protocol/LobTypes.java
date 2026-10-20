package com.jdbcagent.core.protocol;

import java.sql.Types;

/**
 * 大字段列的判定和传输上限。
 */
public final class LobTypes {
    /** 二进制按字节、字符类型按字符数。不超过该值且长度已知时仍嵌进 RowSet。 */
    public static final int INLINE_LIMIT = 256 * 1024;

    /** 单次拉取的最大字节数，低于报文 16MB 上限。 */
    public static final int CHUNK_SIZE = 1024 * 1024;

    private LobTypes() {
    }

    public static boolean isLob(int sqlType) {
        return sqlType == Types.BLOB
                || sqlType == Types.CLOB
                || sqlType == Types.NCLOB
                || sqlType == Types.LONGVARBINARY
                || sqlType == Types.LONGVARCHAR;
    }

    public static boolean isCharacter(int sqlType) {
        return sqlType == Types.CLOB || sqlType == Types.NCLOB || sqlType == Types.LONGVARCHAR;
    }

    /**
     * @param length 驱动返回的长度。未知时为负数
     */
    public static boolean inline(long length) {
        return length >= 0 && length <= INLINE_LIMIT;
    }
}
