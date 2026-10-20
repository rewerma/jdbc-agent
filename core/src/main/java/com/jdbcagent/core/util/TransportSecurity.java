package com.jdbcagent.core.util;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;

/**
 * 客户端和服务端之间的 TLS。只启用 TLS 1.2 / 1.3，并去掉空加密和匿名套件。
 */
public final class TransportSecurity {
    private TransportSecurity() {
    }

    public static SSLContext serverContext(String keyStorePath, String password, String type) {
        KeyStore keyStore = load(keyStorePath, password, type, "sslKeyStore");
        try {
            KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(keyStore, password(password));
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(factory.getKeyManagers(), null, null);
            return context;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load sslKeyStore: " + keyStorePath, e);
        }
    }

    public static SSLContext clientContext(String trustStorePath, String password, String type) {
        KeyStore trustStore = load(trustStorePath, password, type, "sslTrustStore");
        try {
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, factory.getTrustManagers(), null);
            return context;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load sslTrustStore: " + trustStorePath, e);
        }
    }

    public static SSLEngine serverEngine(SSLContext context) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setNeedClientAuth(false);
        restrict(engine);
        return engine;
    }

    public static SSLEngine clientEngine(SSLContext context, String host, int port) {
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        restrict(engine);
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(parameters);
        return engine;
    }

    public static void restrict(SSLEngine engine) {
        String[] protocols = modern(engine.getSupportedProtocols());
        if (protocols.length > 0) {
            engine.setEnabledProtocols(protocols);
        }
        String[] ciphers = strong(engine.getSupportedCipherSuites());
        if (ciphers.length > 0) {
            engine.setEnabledCipherSuites(ciphers);
        }
    }

    public static void restrict(javax.net.ssl.SSLSocket socket) {
        String[] protocols = modern(socket.getSupportedProtocols());
        if (protocols.length > 0) {
            socket.setEnabledProtocols(protocols);
        }
        String[] ciphers = strong(socket.getSupportedCipherSuites());
        if (ciphers.length > 0) {
            socket.setEnabledCipherSuites(ciphers);
        }
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(parameters);
    }

    private static KeyStore load(String path, String password, String type, String name) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalStateException(name + " is required");
        }
        if (password == null) {
            throw new IllegalStateException(name + "Password is required");
        }
        File file = new File(path);
        if (!file.isFile()) {
            throw new IllegalStateException(name + " does not exist: " + path);
        }
        String storeType = type == null || type.trim().isEmpty() ? "PKCS12" : type.trim();
        try {
            KeyStore keyStore = KeyStore.getInstance(storeType);
            InputStream in = new FileInputStream(file);
            try {
                keyStore.load(in, password(password));
            } finally {
                in.close();
            }
            return keyStore;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load " + name + ": " + path, e);
        }
    }

    private static char[] password(String password) {
        return password.toCharArray();
    }

    private static String[] modern(String[] supported) {
        List<String> enabled = new ArrayList<String>();
        if (supported != null) {
            for (String protocol : supported) {
                if ("TLSv1.2".equals(protocol) || "TLSv1.3".equals(protocol)) {
                    enabled.add(protocol);
                }
            }
        }
        return enabled.toArray(new String[enabled.size()]);
    }

    private static String[] strong(String[] supported) {
        List<String> enabled = new ArrayList<String>();
        if (supported != null) {
            for (String cipher : supported) {
                String upper = cipher.toUpperCase();
                if (upper.contains("NULL") || upper.contains("ANON") || upper.contains("EXPORT")
                        || upper.contains("RC4") || upper.contains("DES") || upper.contains("MD5")) {
                    continue;
                }
                enabled.add(cipher);
            }
        }
        return enabled.toArray(new String[enabled.size()]);
    }
}
