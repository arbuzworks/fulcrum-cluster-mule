/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.store;

import com.fulcrum.mule.cluster.ClusterContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.api.context.MuleContextAware;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.ObjectStoreNotAvaliableException;
import org.mule.api.store.PartitionableObjectStore;
import org.mule.config.i18n.CoreMessages;
import org.mule.util.store.DeserializationPostInitialisable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class ClusterObjectStore<T extends Serializable>
        implements ListableObjectStore<T>, PartitionableObjectStore<T>, MuleContextAware
{
    public static final String DEFAULT_PARTITION_NAME = "default";
    public static final String ALL_PARTITIONS_MAP_NAME = "-allPartitions";
    public static final String PARTITION_MAP_NAME = "-partition-";

    private final Log logger = LogFactory.getLog(getClass());

    private ClusterContext clusterContext = null;
    private MuleContext muleContext;

    private String appName;
    private boolean running;

    private ListableObjectStore<T> store;
    private ListableObjectStore<String> partitionNameToDistributedStoreName;

    public ClusterObjectStore(ClusterContext clusterContext)
    {
        this.clusterContext = clusterContext;
        this.running = true;
    }

    public static String getObjectStorePrefix(MuleContext muleContext)
    {
        return muleContext.getConfiguration().getId() + PARTITION_MAP_NAME;
    }

    @Override
    public void setMuleContext(MuleContext muleContext)
    {
        this.muleContext = muleContext;
        if (appName == null)
        {
            appName = muleContext.getConfiguration().getId();
            store = clusterContext.getDistributedObjectStore(appName + PARTITION_MAP_NAME + DEFAULT_PARTITION_NAME);
            partitionNameToDistributedStoreName = clusterContext.getGlobalDistributedStore(appName + ALL_PARTITIONS_MAP_NAME);
        }
    }

    @Override
    public void open() throws ObjectStoreException
    {
    }

    @Override
    public void close() throws ObjectStoreException
    {
        running = false;
    }

    @Override
    public List<Serializable> allKeys() throws ObjectStoreException
    {
        if (!running)
        {
            return Collections.emptyList();
        }
        return doAllKeys(store);
    }

    private List<Serializable> doAllKeys(ListableObjectStore<T> theMap) throws ObjectStoreException
    {
        return theMap.allKeys();
    }

    @Override
    public boolean contains(Serializable key) throws ObjectStoreException
    {
        return doContains(store, key);
    }

    @Override
    public void clear() throws ObjectStoreException
    {
        doClear(store);
    }

    private boolean doContains(ListableObjectStore<T> theMap, Serializable key) throws ObjectStoreException
    {
        if (!running)
        {
            return false;
        }
        return theMap.contains(key);
    }

    @Override
    public void store(Serializable key, T value) throws ObjectStoreException
    {
        checkRunning();
        doStore(store, key, value);
    }

    private void doStore(ListableObjectStore<T> theMap, Serializable key, T value) throws ObjectStoreException
    {
        checkRunning();
        theMap.store(key, value);
    }

    @Override
    public T retrieve(Serializable key) throws ObjectStoreException
    {
        if (!running)
        {
            return null;
        }
        return doRetrieve(store, key);
    }

    private T doRetrieve(ListableObjectStore<T> theMap, Serializable key) throws ObjectStoreException
    {
        if (!this.running)
        {
            return null;
        }
        T value = theMap.retrieve(key);
        if (value == null)
        {
            throw new ObjectDoesNotExistException(CoreMessages.objectNotFound(key));
        }
        try
        {
            initializeIfDeserializable(value);
        }
        catch (ObjectStoreException e)
        {
            throw new ObjectDoesNotExistException(e);
        }
        return value;
    }

    private void doClear(ListableObjectStore<T> theMap) throws ObjectStoreException
    {
        checkRunning();
        theMap.clear();
    }

    @Override
    public T remove(Serializable key) throws ObjectStoreException
    {
        checkRunning();
        return doRemove(store, key);
    }

    private void checkRunning() throws ObjectStoreNotAvaliableException
    {
        if (!running)
        {
            throw new ObjectStoreNotAvaliableException();
        }
    }

    private T doRemove(ListableObjectStore<T> theMap, Serializable key) throws ObjectStoreException
    {
        checkRunning();
        T current = theMap.remove(key);
        if (current == null)
        {
            logger.warn(CoreMessages.objectNotFound(key));
            //throw new ObjectDoesNotExistException(CoreMessages.objectNotFound(key));
        }
        initializeIfDeserializable(current);
        return current;
    }

    @Override
    public boolean isPersistent()
    {
        return false;
    }

    @Override
    public void open(String storeName) throws ObjectStoreException
    {
    }

    @Override
    public boolean contains(Serializable key, String partitionName) throws ObjectStoreException
    {
        return doContains(getPartitionMap(partitionName), key);
    }

    @Override
    public void store(Serializable key, T value, String partitionName) throws ObjectStoreException
    {
        doStore(getPartitionMap(partitionName), key, value);
    }

    @Override
    public T retrieve(Serializable key, String partitionName) throws ObjectStoreException
    {
        return doRetrieve(getPartitionMap(partitionName), key);
    }

    @Override
    public void clear(String partitionName) throws ObjectStoreException
    {
        doClear(getPartitionMap(partitionName));
    }

    @Override
    public T remove(Serializable key, String partitionName) throws ObjectStoreException
    {
        return doRemove(getPartitionMap(partitionName), key);
    }

    @Override
    public List<Serializable> allKeys(String partitionName) throws ObjectStoreException
    {
        return doAllKeys(getPartitionMap(partitionName));
    }

    @Override
    public List<String> allPartitions() throws ObjectStoreException
    {
        List<Serializable> keys = partitionNameToDistributedStoreName.allKeys();
        List<String> partitions = new ArrayList<>(keys.size());
        for (Serializable key : keys)
        {
            partitions.add(String.valueOf(key));
        }
        return partitions;
    }

    @Override
    public void close(String partitionName) throws ObjectStoreException
    {
        partitionNameToDistributedStoreName.remove(partitionName);
    }

    @Override
    public void disposePartition(String partitionName) throws ObjectStoreException
    {
        getPartitionMap(partitionName).clear();
        partitionNameToDistributedStoreName.remove(partitionName);
    }

    public void dispose()
    {
        clusterContext = null;
    }

    private synchronized ListableObjectStore<T> getPartitionMap(String partitionName) throws ObjectStoreException
    {
        String encodedPartitionName = partitionMapKey(partitionName);
        if (!partitionNameToDistributedStoreName.contains(partitionName))
        {
            partitionNameToDistributedStoreName.store(partitionName, encodedPartitionName);
        }
        return clusterContext.getDistributedObjectStore(encodedPartitionName);
    }

    private String partitionMapKey(String partitionName)
    {
        return getObjectStorePrefix(muleContext) + partitionName;
    }

    private void initializeIfDeserializable(T value) throws ObjectStoreException
    {
        if ((value instanceof DeserializationPostInitialisable))
        {
            try
            {
                DeserializationPostInitialisable.Implementation.init(value, muleContext);
            }
            catch (Exception e)
            {
                throw new ObjectStoreException(e);
            }
        }
    }
}
