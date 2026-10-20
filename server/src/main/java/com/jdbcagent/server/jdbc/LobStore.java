package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.LobTypes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 大字段临时文件。进程启动时清掉上次残留，结果集或通道关闭时删除本次文件。
 */
public final class LobStore {
    private static final AtomicLong IDS = new AtomicLong(0);
    private static final ConcurrentHashMap<Long, StoredLob> FILES = new ConcurrentHashMap<Long, StoredLob>();
    private static final ConcurrentHashMap<Long, Set<Long>> BY_RESULT_SET =
            new ConcurrentHashMap<Long, Set<Long>>();
    private static final File DIRECTORY = new File(System.getProperty("java.io.tmpdir"), "jdbc-agent-lob");

    static {
        DIRECTORY.mkdirs();
        sweepQuietly();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                deleteAll();
            }
        }, "jdbc-agent-lob-cleanup"));
    }

    private LobStore() {
    }

    public static final class Spool {
        public final long lobId;
        public final long length;

        Spool(long lobId, long length) {
            this.lobId = lobId;
            this.length = length;
        }
    }

    public static final class Chunk {
        public final byte[] data;
        public final boolean eof;

        Chunk(byte[] data, boolean eof) {
            this.data = data;
            this.eof = eof;
        }
    }

    public static Spool spool(long resultSetId, InputStream input) throws SQLException {
        StoredLob stored = create(resultSetId);
        long total = 0;
        try {
            FileOutputStream out = new FileOutputStream(stored.file);
            try {
                byte[] buffer = new byte[8192];
                int n;
                while ((n = input.read(buffer)) >= 0) {
                    out.write(buffer, 0, n);
                    total += n;
                }
            } finally {
                out.close();
            }
            stored.open(total);
            return new Spool(stored.id, total);
        } catch (IOException e) {
            discard(stored);
            throw new SQLException("failed to spool binary large object", e);
        } finally {
            closeQuietly(input);
        }
    }

    public static Spool spool(long resultSetId, Reader reader) throws SQLException {
        StoredLob stored = create(resultSetId);
        long chars = 0;
        try {
            FileOutputStream out = new FileOutputStream(stored.file);
            Writer writer = new OutputStreamWriter(out, "UTF-8");
            try {
                char[] buffer = new char[8192];
                int n;
                while ((n = reader.read(buffer)) >= 0) {
                    writer.write(buffer, 0, n);
                    chars += n;
                }
                writer.flush();
            } finally {
                writer.close();
            }
            stored.open(stored.file.length());
            return new Spool(stored.id, chars);
        } catch (IOException e) {
            discard(stored);
            throw new SQLException("failed to spool character large object", e);
        } finally {
            closeQuietly(reader);
        }
    }

    public static Chunk read(long lobId, long position, int length) throws SQLException {
        if (position < 0) {
            throw new SQLException("invalid large object position: " + position);
        }
        int asked = length;
        if (asked <= 0 || asked > LobTypes.CHUNK_SIZE) {
            asked = LobTypes.CHUNK_SIZE;
        }
        StoredLob stored = FILES.get(lobId);
        if (stored == null) {
            throw new SQLException("large object not found: " + lobId);
        }
        try {
            return stored.read(position, asked);
        } catch (IOException e) {
            throw new SQLException("failed to read large object " + lobId, e);
        }
    }

    public static void delete(long lobId) {
        StoredLob stored = FILES.remove(lobId);
        if (stored == null) {
            return;
        }
        Set<Long> ids = BY_RESULT_SET.get(stored.resultSetId);
        if (ids != null) {
            synchronized (ids) {
                ids.remove(lobId);
            }
        }
        stored.delete();
    }

    public static void deleteByResultSet(long resultSetId) {
        Set<Long> ids = BY_RESULT_SET.remove(resultSetId);
        if (ids == null) {
            return;
        }
        Long[] snapshot;
        synchronized (ids) {
            snapshot = ids.toArray(new Long[ids.size()]);
            ids.clear();
        }
        for (Long lobId : snapshot) {
            StoredLob stored = FILES.remove(lobId);
            if (stored != null) {
                stored.delete();
            }
        }
    }

    static boolean exists(long lobId) {
        StoredLob stored = FILES.get(lobId);
        return stored != null && stored.file.exists();
    }

    private static StoredLob create(long resultSetId) throws SQLException {
        if (!DIRECTORY.isDirectory() && !DIRECTORY.mkdirs()) {
            throw new SQLException("failed to create large object directory");
        }
        long id = IDS.incrementAndGet();
        File file = new File(DIRECTORY, id + ".lob");
        StoredLob stored = new StoredLob(id, resultSetId, file);
        FILES.put(id, stored);
        Set<Long> created = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        Set<Long> existing = BY_RESULT_SET.putIfAbsent(resultSetId, created);
        Set<Long> ids = existing == null ? created : existing;
        synchronized (ids) {
            ids.add(id);
        }
        return stored;
    }

    private static void discard(StoredLob stored) {
        FILES.remove(stored.id);
        Set<Long> ids = BY_RESULT_SET.get(stored.resultSetId);
        if (ids != null) {
            synchronized (ids) {
                ids.remove(stored.id);
            }
        }
        stored.delete();
    }

    private static void deleteAll() {
        Long[] ids = FILES.keySet().toArray(new Long[FILES.size()]);
        for (Long id : ids) {
            delete(id);
        }
        sweepQuietly();
    }

    private static void sweepQuietly() {
        File[] files = DIRECTORY.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.getName().endsWith(".lob")) {
                file.delete();
            }
        }
    }

    private static void closeQuietly(InputStream input) {
        if (input == null) {
            return;
        }
        try {
            input.close();
        } catch (IOException ignored) {
            // 数据库流关闭失败不影响已经落盘的内容
        }
    }

    private static void closeQuietly(Reader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (IOException ignored) {
            // 数据库流关闭失败不影响已经落盘的内容
        }
    }

    private static final class StoredLob {
        private final long id;
        private final long resultSetId;
        private final File file;
        private RandomAccessFile raf;
        private long size;

        private StoredLob(long id, long resultSetId, File file) {
            this.id = id;
            this.resultSetId = resultSetId;
            this.file = file;
        }

        private void open(long size) throws IOException {
            this.size = size;
            this.raf = new RandomAccessFile(file, "r");
        }

        private synchronized Chunk read(long position, int length) throws IOException {
            if (raf == null) {
                throw new IOException("large object is closed: " + id);
            }
            if (position >= size) {
                return new Chunk(new byte[0], true);
            }
            int toRead = (int) Math.min(length, size - position);
            byte[] data = new byte[toRead];
            raf.seek(position);
            raf.readFully(data);
            return new Chunk(data, position + toRead >= size);
        }

        private synchronized void delete() {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException ignored) {
                    // 关闭失败时仍尝试删除文件
                }
                raf = null;
            }
            file.delete();
        }
    }
}
