package com.jdbcagent.server.netty;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * 测试用的临时 PKCS12。证书 SAN 包含 127.0.0.1 和 localhost。
 */
public final class TestCertificates {
    public static final String PASSWORD = "changeit";

    private TestCertificates() {
    }

    public static File keyStore() throws Exception {
        File file = File.createTempFile("jdbc-agent-", ".p12");
        if (!file.delete()) {
            throw new IllegalStateException("failed to remove empty keystore: " + file);
        }
        file.deleteOnExit();
        String keytool = new File(System.getProperty("java.home"), "bin/keytool").getAbsolutePath();
        Process process = new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias", "jdbc-agent",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-storetype", "PKCS12",
                "-keystore", file.getAbsolutePath(),
                "-storepass", PASSWORD,
                "-keypass", PASSWORD,
                "-dname", "CN=127.0.0.1",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-validity", "1",
                "-noprompt").start();
        String output = read(process.getInputStream()) + read(process.getErrorStream());
        int code = process.waitFor();
        if (code != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
        return file;
    }

    private static String read(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int n;
        while ((n = in.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
