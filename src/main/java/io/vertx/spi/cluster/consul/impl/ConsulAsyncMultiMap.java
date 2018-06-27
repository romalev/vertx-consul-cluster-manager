package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ConsulAsyncMultiMap<K, V> extends ConsulAsyncAbstractMap<K, V> implements AsyncMultiMap<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

    private final ConsulClient consulClient;
    private final Vertx vertx;
    private final String name;

    // TODO: consider adding a cache in cast the connection between node and consul in unstable.
    private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

    public ConsulAsyncMultiMap(String name, ConsulClient consulClient, Vertx vertx) {
        this.name = name;
        this.consulClient = consulClient;
        this.vertx = vertx;
    }

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
        log.trace("Getting an entry by K: '{}' from Consul Async KV store.", this.name);
        assertKeyIsNotNull(k).compose(aVoid -> {
            Future<ChoosableIterable<V>> future = Future.future();
            consulClient.getValues(this.name, resultHandler -> {
                if (resultHandler.succeeded()) {
                    log.trace("Got: {} by entry K: '{}'", resultHandler.result().getList(), this.name);
                    Map<String, String> resultMap = resultHandler.result().getList().stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                    ChoosableSet<V> newEntries = new ChoosableSet<>(resultMap != null ? resultMap.size() : 0);
                    future.complete(newEntries);
                } else {
                    future.fail(resultHandler.cause());
                }
            });
            return future;
        }).setHandler(asyncResultHandler);
    }

    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {

    }

    @Override
    public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {

    }


}
