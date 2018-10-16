package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Consul-based implementation of an asynchronous exclusive lock which can be obtained from any node in the cluster.
 * When the lock is obtained (acquired), no-one else in the cluster can obtain the lock with the same name until the lock is released.
 * <p>
 * <b> Given implementation is based on using consul sessions - see https://www.consul.io/docs/guides/leader-election.html.</b>
 * Some notes:
 * <p>
 * The state of our lock would then correspond to the existence or non-existence of the respective key in the key-value store.
 * In order to obtain the lock we create a simple kv pair in Consul kv store and bound it with ttl session's id.
 * In order to release the lock we remove respective entry (we don't destroy respective ttl session which triggers automatically the deleting of kv pair that was bound to it).
 * <p>
 * Some additional details:
 * https://github.com/hashicorp/consul/issues/432
 * https://blog.emilienkenler.com/2017/05/20/distributed-locking-with-consul/
 * <p>
 * Note: given implementation doesn't require to serialize/deserialize lock related data, instead it just manipulates plain strings.
 *
 * @author Roman Levytskyi
 */
public class ConsulLock extends ConsulMap<String, String> implements Lock {

    private static final Logger log = LoggerFactory.getLogger(ConsulLock.class);

    private final String lockName;
    // lock MUST be bound to tcp check since failed (failing) node must give up any locks being held by it,
    // therefore given session id is already bounded to tcp check.
    private String sessionId;

    public ConsulLock(String name, String nodeId, String sessionId, Vertx vertx, ConsulClient consulClient) {
        super("__vertx.locks", nodeId, vertx, consulClient);
        this.lockName = name;
        this.sessionId = sessionId;
    }


    /**
     * Tries to obtain a lock. Note: Call is BLOCKING!!!
     *
     * @return true - lock has been successfully obtained, false - otherwise.
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public boolean tryObtain() throws InterruptedException, TimeoutException, ExecutionException {
        log.trace("[" + nodeId + "]" + " is trying to obtain a lock on: " + lockName);
        CompletableFuture<Boolean> futureObtainedLock = new CompletableFuture<>();
        obtain().setHandler(event -> {
            if (event.succeeded()) futureObtainedLock.complete(event.result());
            else futureObtainedLock.completeExceptionally(event.cause());
        });

        boolean lockObtained = futureObtainedLock.get(1000, TimeUnit.MILLISECONDS);
        if (lockObtained) {
            log.info("Lock on: " + lockName + " has been obtained.");
        }
        return lockObtained;
    }

    @Override
    public void release() {
        vertx.executeBlocking(event ->
                removeConsulValue(keyPath(lockName)).setHandler(removeResult -> {
                    if (removeResult.succeeded()) {
                        log.info("Lock on: " + lockName + " has been released.");
                        event.complete();
                    } else {
                        throw new VertxException("Failed to release a lock on: " + lockName, removeResult.cause());
                    }
                }), false, null);
    }

    /**
     * Obtains the lock asynchronously.
     */
    private Future<Boolean> obtain() {
        return putConsulValue(keyPath(lockName), "lockAcquired", new KeyValueOptions().setAcquireSession(sessionId));
    }
}