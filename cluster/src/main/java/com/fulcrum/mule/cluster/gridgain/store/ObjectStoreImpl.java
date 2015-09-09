/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.gridgain.store;

import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheEntry;
import org.gridgain.grid.lang.GridPredicate;
import org.mule.api.MuleContext;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.transformer.Transformer;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.simple.ByteArrayToObject;
import org.mule.transformer.simple.ObjectToByteArray;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by arbuzworks on 3/16/15.
 **/
public class ObjectStoreImpl<V extends Serializable> implements ListableObjectStore<V>
{

    private final Transformer serializer;
    private final Transformer deserializer;

    private final String keyPrefix;
    private final GridCache<ObjectStoreKey<Serializable>, byte[]> cache;

    public ObjectStoreImpl(MuleContext muleContext, String keyPrefix, GridCache<ObjectStoreKey<Serializable>, byte[]> cache)
    {
        this.serializer = new ObjectToByteArray();
        this.serializer.setMuleContext(muleContext);
        this.deserializer = new ByteArrayToObject();
        this.deserializer.setMuleContext(muleContext);
        this.keyPrefix = keyPrefix;
        this.cache = cache;
    }

    @Override
    public void open() throws ObjectStoreException
    {
    }

    @Override
    public void close() throws ObjectStoreException
    {
    }

    @Override
    public List<Serializable> allKeys() throws ObjectStoreException
    {
        Iterator<GridCacheEntry<ObjectStoreKey<Serializable>, byte[]>> iterator = cache.iterator();
        ArrayList<Serializable> result = new ArrayList<>(cache.size());
        while (iterator.hasNext())
        {
            GridCacheEntry<ObjectStoreKey<Serializable>, byte[]> cacheEntry = iterator.next();
            if (keyPrefix.equals(cacheEntry.getKey().getKeyPrefix()))
            {
                result.add(cacheEntry.getKey().getKey());
            }
        }
        return result;
    }

    @Override
    public boolean contains(Serializable key) throws ObjectStoreException
    {
        return cache.containsKey(new ObjectStoreKey<>(keyPrefix, key));
    }

    @Override
    public void store(Serializable key, V value) throws ObjectStoreException
    {
        try
        {
            cache.putIfAbsent(new ObjectStoreKey<>(keyPrefix, key), (byte[]) serializer.transform(value));
        }
        catch (TransformerException e)
        {
            throw new ObjectStoreException(e);
        }
        catch (GridException e)
        {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public V retrieve(Serializable key) throws ObjectStoreException
    {
        try
        {
            byte[] retrive = cache.get(new ObjectStoreKey<>(keyPrefix, key));
            if (retrive != null)
            {
                return (V) deserializer.transform(retrive);
            }
            return null;
        }
        catch (TransformerException e)
        {
            throw new ObjectStoreException(e);
        }
        catch (GridException e)
        {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public V remove(Serializable key) throws ObjectStoreException
    {
        try
        {
            byte[] remove = cache.remove(new ObjectStoreKey<>(keyPrefix, key));
            if (remove != null)
            {
                return (V) deserializer.transform(remove);
            }
            return null;
        }
        catch (TransformerException e)
        {
            throw new ObjectStoreException(e);
        }
        catch (GridException e)
        {
            throw new ObjectStoreException(e);
        }
    }

    @Override
    public boolean isPersistent()
    {
        return false;
    }

    @Override
    public void clear() throws ObjectStoreException
    {
        try
        {
            cache.removeAll(new GridPredicate<GridCacheEntry<ObjectStoreKey<Serializable>, byte[]>>()
            {
                @Override
                public boolean apply(GridCacheEntry<ObjectStoreKey<Serializable>, byte[]> entry)
                {
                    return keyPrefix.equals(entry.getKey().getKeyPrefix());
                }
            });
        }
        catch (GridException e)
        {
            throw new ObjectStoreException(e);
        }
    }

}
