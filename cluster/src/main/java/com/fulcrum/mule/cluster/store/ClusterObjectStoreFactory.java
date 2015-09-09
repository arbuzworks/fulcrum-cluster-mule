/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.store;

import com.fulcrum.mule.cluster.ClusterContext;
import org.mule.api.store.ObjectStore;
import org.mule.api.store.QueueStore;
import org.mule.util.store.MuleDefaultObjectStoreFactory;

import java.io.Serializable;

/**
 * Created by arbuzworks on 3/17/15.
 **/
public class ClusterObjectStoreFactory extends MuleDefaultObjectStoreFactory
{

    private final ClusterContext clusterContext;

    public ClusterObjectStoreFactory(ClusterContext clusterContext)
    {
        this.clusterContext = clusterContext;
    }

    @Override
    public ObjectStore<Serializable> createDefaultInMemoryObjectStore()
    {
        return createObjectStore(super.createDefaultInMemoryObjectStore());
    }

    @Override
    public ObjectStore<Serializable> createDefaultPersistentObjectStore()
    {
        return createObjectStore(super.createDefaultPersistentObjectStore());
    }

    @Override
    public QueueStore<Serializable> createDefaultInMemoryQueueStore()
    {
        throw new UnsupportedOperationException("Deprecated this class will be removed in Mule 4.0 in favor of the new queue implementation");
    }

    @Override
    public QueueStore<Serializable> createDefaultPersistentQueueStore()
    {
        throw new UnsupportedOperationException("Deprecated this class will be removed in Mule 4.0 in favor of the new queue implementation");
    }

    @Override
    public ObjectStore<Serializable> createDefaultUserObjectStore()
    {
        return createObjectStore(super.createDefaultUserObjectStore());
    }

    @Override
    public ObjectStore<Serializable> createDefaultUserTransientObjectStore()
    {
        return createObjectStore(super.createDefaultUserTransientObjectStore());
    }

    private <T extends Serializable> ClusterObjectStore<T> createObjectStore(ObjectStore objectStore)
    {
        ClusterObjectStore<T> store = new ClusterObjectStore<T>(clusterContext);
        clusterContext.registerObjectStore(store);
        return store;
    }
}

