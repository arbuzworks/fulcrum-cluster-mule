package com.fulcrum.mule.cluster.context;

import com.fulcrum.mule.cluster.FulcrumCluster;
import com.fulcrum.mule.cluster.FulcrumClusterManager;
import com.fulcrum.mule.cluster.config.FulcrumClusterProperties;
import com.fulcrum.mule.cluster.queue.FulcrumClusterQueueManager;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionFactory;
import com.fulcrum.mule.cluster.store.FulcrumClusterObjectStore;
import com.fulcrum.mule.cluster.store.FulcrumClusterObjectStoreFactory;
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
 * Created on Feb 23, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterApplicationContext {

    private final List<FulcrumClusterObjectStore> objectStorePerApplication = new LinkedList<>();
    private final List<QueueStore> queueStorePerApplication = new LinkedList<>();
    private final HashMap<String, ListableObjectStore> distributedObjectStorePerApplication = new HashMap<>();

    public final String artifactName;
    public final DefaultMuleContext muleContext;

    public FulcrumClusterApplicationContext(String artifactName, MuleContext muleContext) {
        this.artifactName = artifactName;
        this.muleContext = (DefaultMuleContext) muleContext;
    }

    public void initialise(FulcrumClusterManager fulcrumClusterManager) throws RegistrationException {
        muleContext.setPollingController(fulcrumClusterManager);

        FulcrumCluster fulcrumCluster = fulcrumClusterManager.getCluster();

        MuleRegistry registry = muleContext.getRegistry();
        registry.registerObject(FulcrumClusterProperties.MULE_CLUSTER_MANAGER, fulcrumCluster);
        registry.registerObject(FulcrumClusterProperties.OBJECT_CLUSTER_CONFIGURATION, fulcrumClusterManager.getFulcrumClusterConfiguration());

        FulcrumClusterObjectStoreFactory storeFactory = new FulcrumClusterObjectStoreFactory(this);
        registry.registerObject(FulcrumClusterProperties.OBJECT_STORE_DEFAULT_IN_MEMORY_NAME, storeFactory.createDefaultInMemoryObjectStore());
        registry.registerObject(FulcrumClusterProperties.OBJECT_STORE_DEFAULT_PERSISTENT_NAME, storeFactory.createDefaultPersistentObjectStore());
        registry.registerObject(FulcrumClusterProperties.DEFAULT_USER_OBJECT_STORE_NAME, storeFactory.createDefaultUserObjectStore());
        registry.registerObject(FulcrumClusterProperties.DEFAULT_USER_TRANSIENT_OBJECT_STORE_NAME, storeFactory.createDefaultUserTransientObjectStore());

        muleContext.setQueueManager(new FulcrumClusterQueueManager(this));
    }

    public void preDispose() {
        synchronized (objectStorePerApplication) {
            for (FulcrumClusterObjectStore objectStore : objectStorePerApplication) {
                try {
                    objectStore.close();
                } catch (ObjectStoreException e) {
                    //ignored
                }
            }
        }
        synchronized (queueStorePerApplication) {
            for (QueueStore queueStore : queueStorePerApplication) {
                queueStore.close();
            }
        }
    }

    public void dispose() {
        synchronized (objectStorePerApplication) {
            for (FulcrumClusterObjectStore objectStore : objectStorePerApplication) {
                try {
                    objectStore.close();
                    objectStore.dispose();
                } catch (ObjectStoreException e) {
                    //ignored
                }
            }
            objectStorePerApplication.clear();
        }
        synchronized (queueStorePerApplication) {
            for (QueueStore queueStore : queueStorePerApplication) {
                queueStore.close();
                queueStore.dispose();
            }
            queueStorePerApplication.clear();
        }
        synchronized (distributedObjectStorePerApplication) {
            distributedObjectStorePerApplication.clear();
        }
        unregisterObject(FulcrumClusterProperties.MULE_CLUSTER_MANAGER);
        unregisterObject(FulcrumClusterProperties.OBJECT_CLUSTER_CONFIGURATION);
    }

    public <T extends Serializable> void registerObjectStore(FulcrumClusterObjectStore<T> store) {
        synchronized (objectStorePerApplication) {
            objectStorePerApplication.add(store);
        }
    }

    private void unregisterObject(String key) {
        try {
            muleContext.getRegistry().unregisterObject(key);
        } catch (RegistrationException e) {
            //ignored
        }
    }

    public void fireNotification(ServerNotification serverNotification) {
        muleContext.fireNotification(serverNotification);
    }

    public QueueStore distributedQueue(String queueName, QueueConfiguration config, boolean collocated) {
        FulcrumCluster fulcrumCluster = muleContext.getRegistry().get(FulcrumClusterProperties.MULE_CLUSTER_MANAGER);
        QueueStore gridCacheQueueStore = fulcrumCluster.queue(muleContext, queueName, config, collocated);
        synchronized (queueStorePerApplication) {
            queueStorePerApplication.add(gridCacheQueueStore);
        }
        return gridCacheQueueStore;
    }

    public FulcrumTransactionFactory transactionFactory() {
        FulcrumCluster fulcrumCluster = muleContext.getRegistry().get(FulcrumClusterProperties.MULE_CLUSTER_MANAGER);
        return fulcrumCluster.getTransactionFactory();
    }

    public <T extends Serializable> ListableObjectStore<T> distributedStore(String keyPrefix) {
        synchronized (distributedObjectStorePerApplication) {
            ListableObjectStore<T> listableObjectStore = distributedObjectStorePerApplication.get(keyPrefix);
            if (listableObjectStore == null) {
                FulcrumCluster fulcrumCluster = muleContext.getRegistry().get(FulcrumClusterProperties.MULE_CLUSTER_MANAGER);
                listableObjectStore = fulcrumCluster.store(keyPrefix);
                distributedObjectStorePerApplication.put(keyPrefix, listableObjectStore);
            }
            return listableObjectStore;
        }
    }

    public <T extends Serializable> ListableObjectStore<T> globalDistributedStore(String keyPrefix) {
        FulcrumCluster fulcrumCluster = muleContext.getRegistry().get(FulcrumClusterProperties.MULE_CLUSTER_MANAGER);
        return fulcrumCluster.store(keyPrefix);
    }

}
