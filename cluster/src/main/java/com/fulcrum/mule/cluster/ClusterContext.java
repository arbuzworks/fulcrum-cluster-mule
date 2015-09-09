/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster;

import com.fulcrum.mule.cluster.config.ClusterProperties;
import com.fulcrum.mule.cluster.queue.ClusterQueueManager;
import com.fulcrum.mule.cluster.queue.transaction.TransactionFactory;
import com.fulcrum.mule.cluster.store.ClusterObjectStore;
import com.fulcrum.mule.cluster.store.ClusterObjectStoreFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.DefaultMuleContext;
import org.mule.api.MuleContext;
import org.mule.api.context.notification.ServerNotification;
import org.mule.api.registry.MuleRegistry;
import org.mule.api.registry.RegistrationException;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by arbuzworks on 3/13/15.
 **/
public class ClusterContext
{
    private final Log logger = LogFactory.getLog(getClass());

    private final List<ClusterObjectStore> objectStores = new LinkedList<>();
    private final List<QueueStore> queueStores = new LinkedList<>();
    private final HashMap<String, ListableObjectStore> distributedObjectStores = new HashMap<>();

    private final String artifactName;
    private final DefaultMuleContext muleContext;

    public ClusterContext(String artifactName, MuleContext muleContext)
    {
        this.artifactName = artifactName;
        this.muleContext = (DefaultMuleContext) muleContext;
    }

    public void initialise(Cluster cluster) throws RegistrationException
    {
        muleContext.setPollingController(cluster);

        MuleRegistry registry = muleContext.getRegistry();
        registry.registerObject(ClusterProperties.MULE_CLUSTER_MANAGER, cluster);
        registry.registerObject(ClusterProperties.OBJECT_CLUSTER_CONFIGURATION, cluster.getConfiguration());

        ClusterObjectStoreFactory storeFactory = new ClusterObjectStoreFactory(this);
        registry.registerObject(ClusterProperties.OBJECT_STORE_DEFAULT_IN_MEMORY_NAME, storeFactory.createDefaultInMemoryObjectStore());
        registry.registerObject(ClusterProperties.OBJECT_STORE_DEFAULT_PERSISTENT_NAME, storeFactory.createDefaultPersistentObjectStore());
        registry.registerObject(ClusterProperties.DEFAULT_USER_OBJECT_STORE_NAME, storeFactory.createDefaultUserObjectStore());
        registry.registerObject(ClusterProperties.DEFAULT_USER_TRANSIENT_OBJECT_STORE_NAME, storeFactory.createDefaultUserTransientObjectStore());

        muleContext.setQueueManager(new ClusterQueueManager(this));
    }


    public <T extends Serializable> void registerObjectStore(ClusterObjectStore<T> store)
    {
        synchronized (objectStores)
        {
            objectStores.add(store);
        }
    }

    private void unregisterObject(String key)
    {
        try
        {
            muleContext.getRegistry().unregisterObject(key);
        }
        catch (RegistrationException e)
        {
            //TODO
            logger.warn("Failed to unregister object");
        }
    }

    public void fireNotification(ServerNotification serverNotification)
    {
        muleContext.fireNotification(serverNotification);
    }

    public QueueStore getDistributedQueueStore(String queueName, QueueConfiguration config, boolean collocated, boolean create)
    {
        Cluster cluster = muleContext.getRegistry().get(ClusterProperties.MULE_CLUSTER_MANAGER);
        QueueStore queueStore = cluster.getQueueStore(muleContext, queueName, config, collocated, create);
        synchronized (queueStores)
        {
            queueStores.add(queueStore);
        }
        return queueStore;
    }

    public TransactionFactory getTransactionFactory()
    {
        Cluster cluster = muleContext.getRegistry().get(ClusterProperties.MULE_CLUSTER_MANAGER);
        return cluster.getTransactionFactory();
    }

    public <T extends Serializable> ListableObjectStore<T> getDistributedObjectStore(String keyPrefix)
    {
        synchronized (objectStores)
        {
            ListableObjectStore<T> objectStore = distributedObjectStores.get(keyPrefix);
            if (objectStore == null)
            {
                Cluster cluster = muleContext.getRegistry().get(ClusterProperties.MULE_CLUSTER_MANAGER);
                objectStore = cluster.getObjectStore(muleContext, keyPrefix);
                distributedObjectStores.put(keyPrefix, objectStore);
            }
            return objectStore;
        }
    }

    public <T extends Serializable> ListableObjectStore<T> getGlobalDistributedStore(String keyPrefix)
    {
        Cluster cluster = muleContext.getRegistry().get(ClusterProperties.MULE_CLUSTER_MANAGER);
        return cluster.getObjectStore(muleContext, keyPrefix);
    }

    public void close()
    {
        synchronized (objectStores)
        {
            for (ClusterObjectStore objectStore : objectStores)
            {
                try
                {
                    objectStore.close();
                }
                catch (ObjectStoreException e)
                {
                    logger.warn("Failed to close");
                }
            }
        }
        synchronized (queueStores)
        {
            for (QueueStore queueStore : queueStores)
            {
                queueStore.close();
            }
        }
    }

    public void dispose()
    {
        synchronized (objectStores)
        {
            for (ClusterObjectStore objectStore : objectStores)
            {
                try
                {
                    objectStore.close();
                    objectStore.dispose();
                }
                catch (ObjectStoreException e)
                {
                    logger.warn("Failed to dispose");
                }
            }
            objectStores.clear();
        }
        synchronized (queueStores)
        {
            for (QueueStore queueStore : queueStores)
            {
                queueStore.close();
                queueStore.dispose();
            }
            queueStores.clear();
        }
        synchronized (distributedObjectStores)
        {
            distributedObjectStores.clear();
        }
        unregisterObject(ClusterProperties.MULE_CLUSTER_MANAGER);
        unregisterObject(ClusterProperties.OBJECT_CLUSTER_CONFIGURATION);
    }
}
