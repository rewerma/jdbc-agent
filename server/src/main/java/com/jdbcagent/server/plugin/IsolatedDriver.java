package com.jdbcagent.server.plugin;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * 把插件类加载器里的驱动包成 server 类加载器能看见的驱动。
 * DriverManager 只接受调用方类加载器装得出来的驱动，插件里的原始驱动对 server 是不可见的。
 */
final class IsolatedDriver implements Driver {

    private final Driver delegate;

    IsolatedDriver(Driver delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("driver is null");
        }
        this.delegate = delegate;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.connect(url, info);
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.acceptsURL(url);
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.getPropertyInfo(url, info);
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public int getMajorVersion() {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.getMajorVersion();
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public int getMinorVersion() {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.getMinorVersion();
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public boolean jdbcCompliant() {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.jdbcCompliant();
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        ClassLoader previous = pushDriverLoader();
        try {
            return delegate.getParentLogger();
        } finally {
            popDriverLoader(previous);
        }
    }

    @Override
    public String toString() {
        return "IsolatedDriver[" + delegate.getClass().getName() + "]";
    }

    private ClassLoader pushDriverLoader() {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(delegate.getClass().getClassLoader());
        return previous;
    }

    private static void popDriverLoader(ClassLoader previous) {
        Thread.currentThread().setContextClassLoader(previous);
    }
}
