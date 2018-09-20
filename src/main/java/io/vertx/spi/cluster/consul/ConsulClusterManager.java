package io.vertx.spi.cluster.consul;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.impl.*;
import io.vertx.spi.cluster.consul.impl.cache.CacheManager;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cluster manager that uses Consul. Given implementation is based vertx consul client.
 * Current restrictions :
 * <p>
 * - The limit on a key's value size of any of the consul maps is 512KB. This is strictly enforced and an HTTP 413 status will be returned to
 * any client that attempts to store more than that limit in a value. It should be noted that the Consul key/value store is not designed to be used as a general purpose database.
 * <p>
 * See README to get more details.
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager implements ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);
    private final String nodeId;
    private final ConsulClientOptions cClOptns;
    private final Map<String, ConsulLock> locks = new ConcurrentHashMap<>();
    private final Map<String, ConsulCounter> counters = new ConcurrentHashMap<>();
    private final Map<String, AsyncMap<?, ?>> asyncMaps = new ConcurrentHashMap<>();
    private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMaps = new ConcurrentHashMap<>();
    private Vertx vertx;
    private ConsulClient cC;
    private ConsulNodeManager nM;
    private CacheManager cM;
    private volatile boolean active;

    public ConsulClusterManager(final ConsulClientOptions options) {
        Objects.requireNonNull(options, "Consul client options can't be null");
        this.cClOptns = options;
        this.nodeId = UUID.randomUUID().toString();
    }

    public ConsulClusterManager() {
        this.cClOptns = new ConsulClientOptions();
        this.nodeId = UUID.randomUUID().toString();
    }

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Every eventbus handler has an ID. SubsMap (subscriber map) is a MultiMap which
     * maps handler-IDs with server-IDs and thus allows the eventbus to determine where
     * to send messages.
     *
     * @param name A unique name by which the the MultiMap can be identified within the cluster.
     * @return subscription map
     */
    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async multimap by name: '{}'", name);
        Future<AsyncMultiMap<K, V>> futureMultiMap = Future.future();
        AsyncMultiMap asyncMultiMap = asyncMultiMaps.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, vertx, cC, cM, nM.getSessionId(), nodeId));
        futureMultiMap.complete(asyncMultiMap);
        futureMultiMap.setHandler(asyncResultHandler);
    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async map by name: '{}'", name);
        Future<AsyncMap<K, V>> futureMap = Future.future();
        AsyncMap asyncMap = asyncMaps.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, vertx, cC, cM));
        futureMap.complete(asyncMap);
        futureMap.setHandler(asyncResultHandler);
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        log.trace("Getting sync map by name: '{}' with initial cache: '{}'", name, Json.encodePrettily(nM.getHaInfo()));
        return new ConsulSyncMap<>(name, vertx, cC, cM, nM.getSessionId(), nM.getHaInfo());
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
        log.trace("Getting lock with timeout by name: '{}'", name);
        Future<Lock> futureLock = Future.future();
        Lock lock = locks.computeIfAbsent(name, key -> new ConsulLock(name, timeout, cC));
        futureLock.complete(lock);
        futureLock.setHandler(resultHandler);
    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
        log.trace("Getting counter by name: '{}'", name);
        Future<Counter> counterFuture = Future.future();
        Counter counter = counters.computeIfAbsent(name, key -> new ConsulCounter(name, cC));
        counterFuture.complete(counter);
        counterFuture.setHandler(resultHandler);
    }

    @Override
    public String getNodeID() {
        log.trace("Getting node id: '{}'", nodeId);
        return nodeId;
    }

    @Override
    public List<String> getNodes() {
        return nM.getNodes();
    }

    @Override
    public void nodeListener(NodeListener listener) {
        log.trace("Initializing the node listener...");
        nM.initNodeListener(listener);
    }

    @Override
    public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
        Future<Void> future = Future.future();
        log.trace("'{}' is trying to join the cluster.", nodeId);
        if (!active) {
            active = true;
            try {
                cC = ConsulClient.create(vertx, cClOptns);
                cM = new CacheManager(vertx, cClOptns);
                nM = new ConsulNodeManager(vertx, cC, cM, nodeId);
            } catch (final Exception e) {
                future.fail(e);
            }
            nM.join(future.completer());
        } else {
            log.warn("'{}' is NOT active.", nodeId);
            future.complete();
        }
        future.setHandler(resultHandler);
    }

    @Override
    public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
        Future<Void> resultFuture = Future.future();
        log.trace("'{}' is trying to leave the cluster.", nodeId);
        if (active) {
            active = false;
            cM.close();
            nM.leave(resultFuture.completer());
        } else {
            log.warn("'{}' is NOT active.", nodeId);
            resultFuture.complete();
        }
        resultFuture.setHandler(resultHandler);
    }

    @Override
    public boolean isActive() {
        return active;
    }
}