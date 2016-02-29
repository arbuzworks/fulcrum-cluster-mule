package com.fulcrum.mule.cluster.store;

import com.fulcrum.mule.cluster.context.FulcrumClusterApplicationContext;
import org.mule.api.store.ObjectStore;
import org.mule.api.store.QueueStore;
import org.mule.util.store.MuleDefaultObjectStoreFactory;

import java.io.Serializable;

/**
 * Created on Feb 05, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterObjectStoreFactory extends MuleDefaultObjectStoreFactory {

    private final FulcrumClusterApplicationContext applicationContext;

    public FulcrumClusterObjectStoreFactory(FulcrumClusterApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public ObjectStore<Serializable> createDefaultInMemoryObjectStore() {
        return createObjectStore(super.createDefaultInMemoryObjectStore());
    }

    @Override
    public ObjectStore<Serializable> createDefaultPersistentObjectStore() {
        return createObjectStore(super.createDefaultPersistentObjectStore());
    }

    @Override
    public QueueStore<Serializable> createDefaultInMemoryQueueStore() {
        throw new UnsupportedOperationException("Deprecated this class will be removed in Mule 4.0 in favor of the new queue implementation");
    }

    @Override
    public QueueStore<Serializable> createDefaultPersistentQueueStore() {
        throw new UnsupportedOperationException("Deprecated this class will be removed in Mule 4.0 in favor of the new queue implementation");
    }

    @Override
    public ObjectStore<Serializable> createDefaultUserObjectStore() {
        return createObjectStore(super.createDefaultUserObjectStore());
    }

    @Override
    public ObjectStore<Serializable> createDefaultUserTransientObjectStore() {
        return createObjectStore(super.createDefaultUserTransientObjectStore());
    }

    private <T extends Serializable> FulcrumClusterObjectStore<T> createObjectStore(ObjectStore objectStore) {
        FulcrumClusterObjectStore<T> store = new FulcrumClusterObjectStore<T>(applicationContext);
        applicationContext.registerObjectStore(store);
        return store;
    }
}

