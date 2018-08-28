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
import io.vertx.ext.consul.*;
import io.vertx.spi.cluster.consul.impl.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Cluster manager that uses Consul. Given implementation is based vertx consul client.
 * Current restrictions :
 * <p>
 * - String must be used as a key for async maps. Current {@link ClusterSerializationUtils} must get enhanced to allow custom type be the key.
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
    // dedicated queue to store all the consul watches that belongs to a node - when a node leaves the cluster - all its appropriate watches must be stopped.
    private final Queue<Watch> watches = new ConcurrentLinkedQueue<>();
    private Vertx vertx;
    private ConsulClient cC;
    private ConsulNodeManager nodeManager;
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
        log.trace("Injecting Vert.x instance and Initializing consul client ...");
        this.vertx = vertx;
        this.cC = ConsulClient.create(vertx, cClOptns);
        this.nodeManager = new ConsulNodeManager(vertx, cC, createAndGetNodeWatch(), nodeId);
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
        AsyncMultiMap asyncMultiMap = asyncMultiMaps.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, vertx, cC, nodeManager.getSessionId(), nodeId));
        futureMultiMap.complete(asyncMultiMap);
        futureMultiMap.setHandler(asyncResultHandler);
    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
        log.trace("Getting async map by name: '{}'", name);
        Future<AsyncMap<K, V>> futureMap = Future.future();
        AsyncMap asyncMap = asyncMaps.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, vertx, cC, createAndGetMapWatch(name)));
        futureMap.complete(asyncMap);
        futureMap.setHandler(asyncResultHandler);
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        log.trace("Getting sync map by name: '{}' with initial cache: '{}'", name, Json.encodePrettily(nodeManager.getHaInfo()));
        Watch<KeyValueList> watch = createAndGetMapWatch(name);
        return new ConsulSyncMap<>(name, vertx, cC, watch, nodeManager.getSessionId(), nodeManager.getHaInfo());
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
        return nodeManager.getNodes();
    }

    @Override
    public void nodeListener(NodeListener listener) {
        log.trace("Initializing the node listener...");
        nodeManager.watchNewNodes(listener).start();
    }

    @Override
    public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
        Future<Void> future = Future.future();
        log.trace("'{}' is trying to join the cluster.", nodeId);
        if (!active) {
            active = true;
            nodeManager.join(future.completer());
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
            stopWatches();
            nodeManager.leave(resultFuture.completer());
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

    /**
     * Creates consul (service specific) watch.
     */
    private Watch<ServiceList> createAndGetNodeWatch() {
        Watch<ServiceList> serviceWatch = Watch.services(vertx, cClOptns);
        watches.add(serviceWatch);
        return serviceWatch;
    }

    /**
     * Creates consul (kv store specific) watch.
     */
    private Watch<KeyValueList> createAndGetMapWatch(String mapName) {
        Watch<KeyValueList> kvWatch = Watch.keyPrefix(mapName, vertx, cClOptns);
        watches.add(kvWatch);
        return kvWatch;
    }

    /**
     * Stops all node's watches.
     */
    private void stopWatches() {
        watches.forEach(Watch::stop);
    }
}