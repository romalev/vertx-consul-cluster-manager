package io.vertx.core.shareddata;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ConsulClusteredAsynchronousLockTest extends ClusteredAsynchronousLockTest {

    @Test
    public void testAcquireOnExecuteBlocking() {
        Vertx vertx = getVertx();
        SharedData sharedData = vertx.sharedData();
        AtomicReference<Long> start = new AtomicReference<>();
        vertx.<Lock>executeBlocking(future -> {
            CountDownLatch acquireLatch = new CountDownLatch(1);
            AtomicReference<AsyncResult<Lock>> lockReference = new AtomicReference<>();
            sharedData.getLock("foo", ar -> {
                lockReference.set(ar);
                acquireLatch.countDown();
            });
            try {
                awaitLatch(acquireLatch);
                AsyncResult<Lock> ar = lockReference.get();
                if (ar.succeeded()) {
                    future.complete(ar.result());
                } else {
                    future.fail(ar.cause());
                }
            } catch (InterruptedException e) {
                future.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                start.set(System.currentTimeMillis());
                vertx.setTimer(1000, tid -> {
                    ar.result().release();
                });
                vertx.executeBlocking(future -> {
                    CountDownLatch acquireLatch = new CountDownLatch(1);
                    AtomicReference<AsyncResult<Lock>> lockReference = new AtomicReference<>();
                    sharedData.getLock("foo", ar2 -> {
                        lockReference.set(ar2);
                        acquireLatch.countDown();
                    });
                    try {
                        awaitLatch(acquireLatch);
                        AsyncResult<Lock> ar3 = lockReference.get();
                        if (ar3.succeeded()) {
                            future.complete(ar3.result());
                        } else {
                            future.fail(ar3.cause());
                        }
                    } catch (InterruptedException e) {
                        future.fail(e);
                    }
                }, ar4 -> {
                    if (ar4.succeeded()) {
                        // Should be delayed
                        assertTrue(System.currentTimeMillis() - start.get() >= 1000);
                        testComplete();
                    } else {
                        fail(ar4.cause());
                    }
                });
            } else {
                fail(ar.cause());
            }
        });
        await();
    }

    @Override
    protected ClusterManager getClusterManager() {
        return new ConsulClusterManager(new ConsulClientOptions());
    }
}
