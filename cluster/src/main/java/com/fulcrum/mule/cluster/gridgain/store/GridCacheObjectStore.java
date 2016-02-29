package com.fulcrum.mule.cluster.gridgain.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectStoreException;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.*;

/**
 * Created on Feb 20, 2015
 *
 * @author Andrey Maryshev
 */
public class GridCacheObjectStore<V extends Serializable> implements ListableObjectStore<V> {

    private static final Log LOGGER = LogFactory.getLog(GridCacheObjectStore.class);

    final String keyPrefix;
    final IgniteCache<GridCacheObjectStoreKey<Serializable>, V> cache;

    public GridCacheObjectStore(String keyPrefix, IgniteCache<GridCacheObjectStoreKey<Serializable>, V> cache) {
        this.keyPrefix = keyPrefix;
        this.cache = cache;
    }

    @Override
    public void open() throws ObjectStoreException {
    }

    @Override
    public void close() throws ObjectStoreException {
    }

    @Override
    public List<Serializable> allKeys() throws ObjectStoreException {
        Iterator<Cache.Entry<GridCacheObjectStoreKey<Serializable>, V>> iterator = cache.iterator();
        ArrayList<Serializable> result = new ArrayList<>(cache.size());
        while (iterator.hasNext()) {
            Cache.Entry<GridCacheObjectStoreKey<Serializable>, V> cacheEntry = iterator.next();
            if (keyPrefix.equals(cacheEntry.getKey().keyPrefix)) {
                result.add(cacheEntry.getKey().key);
            }
        }
        return result;
    }

    @Override
    public boolean contains(Serializable key) throws ObjectStoreException {
        return cache.containsKey(new GridCacheObjectStoreKey<Serializable>(keyPrefix, key));
    }

    @Override
    public void store(Serializable key, V value) throws ObjectStoreException {
        try {
            cache.putIfAbsent(new GridCacheObjectStoreKey<Serializable>(keyPrefix, key), value);
        } catch (IgniteException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Store failed. key=" + new GridCacheObjectStoreKey<Serializable>(keyPrefix, key) + ", value=" + value, e);
            }
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public V retrieve(Serializable key) throws ObjectStoreException {
        try {
            return cache.get(new GridCacheObjectStoreKey<Serializable>(keyPrefix, key));
        } catch (IgniteException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Retrieve failed. key=" + new GridCacheObjectStoreKey<Serializable>(keyPrefix, key), e);
            }
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public V remove(Serializable key) throws ObjectStoreException {
        try {
            return cache.getAndRemove(new GridCacheObjectStoreKey<Serializable>(keyPrefix, key));
        } catch (IgniteException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Remove failed. key=" + new GridCacheObjectStoreKey<Serializable>(keyPrefix, key), e);
            }
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public void clear() throws ObjectStoreException {
        IgniteCache<GridCacheObjectStoreKey<Serializable>, V> entries = cache;
        Iterator<Cache.Entry<GridCacheObjectStoreKey<Serializable>, V>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Cache.Entry<GridCacheObjectStoreKey<Serializable>, V> next = iterator.next();
            if (next == null) {
                continue;
            }
            if (keyPrefix.equals(next.getKey().keyPrefix)) {
                iterator.remove();
            }
        }
    }

}