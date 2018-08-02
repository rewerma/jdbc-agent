package com.jdbcagent.core.util;

import java.io.*;

/**
 * JDBC-Agent 工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Util {
    /**
     * 字节流转字节数组
     *
     * @param in 字节流
     * @return 字节数组
     * @throws IOException
     */
    public static byte[] input2Bytes(InputStream in) throws IOException {
        if (in == null) {
            return new byte[0];
        }
        try (ByteArrayOutputStream swapStream = new ByteArrayOutputStream();) {
            byte[] buff = new byte[100];
            int rc = 0;
            while ((rc = in.read(buff, 0, 100)) > 0) {
                swapStream.write(buff, 0, rc);
            }
            return swapStream.toByteArray();
        }
    }

    /**
     * 字节流转字节数组, 指定长度
     *
     * @param in     字节流
     * @param length 长度
     * @return 字节数组
     * @throws IOException
     */
    public static byte[] input2Bytes(InputStream in, int length) throws IOException {
        if (in == null) {
            return new byte[0];
        }
        try (ByteArrayOutputStream swapStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[100];
            int len = -1;
            int temp = length - 100;
            while ((len = in.read(buffer)) != -1) {
                swapStream.write(buffer, 0, len);
                if (swapStream.size() > temp) {
                    if (swapStream.size() == length) {
                        break;
                    }

                    byte[] buffer2 = new byte[length - swapStream.size()];
                    while ((len = in.read(buffer2)) != -1) {
                        swapStream.write(buffer2, 0, len);
                        if (swapStream.size() == length) {
                            break;
                        }
                        buffer2 = new byte[length - swapStream.size()];
                    }

                    break;
                }
            }
            return swapStream.toByteArray();
        }
    }

    /**
     * 字节数组转字节流
     *
     * @param buf 字节数组
     * @return 字节流
     */
    public static InputStream byte2Input(byte[] buf) {
        return new ByteArrayInputStream(buf);
    }

    /**
     * reader转字符串
     *
     * @param reader 字符流
     * @return 字符串
     * @throws IOException
     */
    public static String reader2String(Reader reader) throws IOException {
        if (reader == null) {
            return null;
        }
        try (CharArrayWriter swapWriter = new CharArrayWriter()) {
            StringBuilder buffer = new StringBuilder();

            char[] buff = new char[100];
            int rc = 0;
            while ((rc = reader.read(buff, 0, 100)) > 0) {
                swapWriter.write(buff, 0, rc);
            }
            return new String(swapWriter.toCharArray());
        }
    }

    /**
     * 字符串转字符流
     *
     * @param str 字符串
     * @return
     */
    public static Reader string2Reader(String str) {
        return new StringReader(str);
    }

    /**
     * 字符流转字符串, 指定长度
     *
     * @param reader 字符流
     * @param length 长度
     * @return 字符串
     * @throws IOException
     */
    public static String reader2String(Reader reader, int length) throws IOException {
        if (reader == null) {
            return null;
        }
        try (CharArrayWriter swapWriter = new CharArrayWriter()) {
            char[] buffer = new char[100];
            int len = -1;
            int temp = length - 100;
            while ((len = reader.read(buffer)) != -1) {
                swapWriter.write(buffer, 0, len);
                if (swapWriter.size() > temp) {
                    if (swapWriter.size() == length) {
                        break;
                    }

                    char[] buffer2 = new char[length - swapWriter.size()];
                    while ((len = reader.read(buffer2)) != -1) {
                        swapWriter.write(buffer2, 0, len);
                        if (swapWriter.size() == length) {
                            break;
                        }
                        buffer2 = new char[length - swapWriter.size()];
                    }

                    break;
                }
            }
            return new String(swapWriter.toCharArray());
        }
    }

    /**
     * 字符流转字符数组
     *
     * @param reader 字符流
     * @return 字符数组
     * @throws IOException
     */
    public static char[] reader2Chars(Reader reader) throws IOException {
        if (reader == null) {
            return new char[0];
        }
        return reader2String(reader).toCharArray();
    }

    /**
     * 字符流转字符数组, 指定长度
     *
     * @param reader 字符流
     * @param length 长度
     * @return 字符数组
     * @throws IOException
     */
    public static char[] reader2Chars(Reader reader, int length) throws IOException {
        if (reader == null) {
            return new char[0];
        }
        return reader2String(reader, length).toCharArray();
    }
}
