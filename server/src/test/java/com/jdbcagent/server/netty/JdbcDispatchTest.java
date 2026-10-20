package com.jdbcagent.server.netty;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class JdbcDispatchTest {

    @After
    public void tearDown() {
        JdbcDispatch.shutdown();
    }

    @Test
    public void runsOffTheCallerThread() throws Exception {
        final AtomicReference<String> threadName = new AtomicReference<String>();
        final CountDownLatch done = new CountDownLatch(1);
        JdbcDispatch.execute(new Runnable() {
            @Override
            public void run() {
                threadName.set(Thread.currentThread().getName());
                done.countDown();
            }
        });

        Assert.assertTrue(done.await(2, TimeUnit.SECONDS));
        Assert.assertTrue(threadName.get().startsWith("jdbc-agent-jdbc-"));
        Assert.assertFalse(Thread.currentThread().getName().equals(threadName.get()));
    }

    @Test
    public void differentTasksOverlap() throws Exception {
        final CountDownLatch bothStarted = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        Runnable block = new Runnable() {
            @Override
            public void run() {
                bothStarted.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        JdbcDispatch.execute(block);
        JdbcDispatch.execute(block);

        Assert.assertTrue(bothStarted.await(2, TimeUnit.SECONDS));
        release.countDown();
    }

    @Test
    public void fullQueueRejectsInsteadOfRunningOnCaller() throws Exception {
        ThreadPoolExecutor executor = JdbcDispatch.newExecutor(1, 1);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                started.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        Assert.assertTrue(started.await(2, TimeUnit.SECONDS));
        executor.execute(new Runnable() {
            @Override
            public void run() {
            }
        });

        final String caller = Thread.currentThread().getName();
        final AtomicReference<String> ranOn = new AtomicReference<String>();
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    ranOn.set(Thread.currentThread().getName());
                }
            });
            Assert.fail("full jdbc pool should reject");
        } catch (RejectedExecutionException expected) {
            Assert.assertNull(ranOn.get());
            Assert.assertEquals(caller, Thread.currentThread().getName());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
