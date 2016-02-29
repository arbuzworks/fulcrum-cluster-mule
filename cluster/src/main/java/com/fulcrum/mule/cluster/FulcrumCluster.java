package com.fulcrum.mule.cluster;

import com.fulcrum.mule.cluster.config.FulcrumClusterConfiguration;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionFactory;
import org.mule.api.MuleContext;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.store.ListableObjectStore;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;

/**
 * Created on Feb 24, 2015
 *
 * @author Andrey Maryshev
 */
public interface FulcrumCluster {

    String QUEUE_CACHE = "_defaultQ";
    String STORE_CACHE = "_defaultS";
    String QUEUE_LABEL = "-queue-";

    void initialise(FulcrumClusterConfiguration fulcrumClusterConfiguration) throws InitialisationException;

    void dispose();

    FulcrumTransactionFactory getTransactionFactory();

    void registerClusterTopologyListener(FulcrumPrimaryPollingInstanceListener listener);

    QueueStore queue(MuleContext muleContext, String queueName,
                     QueueConfiguration config, boolean collocated);

    <V extends Serializable> ListableObjectStore<V> store(final String keyPrefix);

}
