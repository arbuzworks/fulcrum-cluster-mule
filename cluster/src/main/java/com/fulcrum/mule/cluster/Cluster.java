/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster;

import com.fulcrum.mule.cluster.config.ClusterConfig;
import com.fulcrum.mule.cluster.exception.ClusterException;
import com.fulcrum.mule.cluster.queue.transaction.TransactionFactory;
import org.mule.api.MuleContext;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.store.ListableObjectStore;
import org.mule.transport.PollingController;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public interface Cluster extends PollingController, PrimaryPollingInstanceListener
{
    String QUEUE_CACHE = "_defaultQ";
    String STORE_CACHE = "_defaultS";
    String QUEUE_LABEL = "-queue-";

    void initialise(ClusterConfig clusterConfiguration) throws InitialisationException;

    ClusterConfig getConfiguration();

    HashMap<String, ClusterContext> getClusterContexts();

    void dispose() throws ClusterException;

    TransactionFactory getTransactionFactory();

    QueueStore getQueueStore(MuleContext muleContext, String queueName,
                     QueueConfiguration config, boolean collocated, boolean create);

    <V extends Serializable> ListableObjectStore<V> getObjectStore(MuleContext muleContext, String keyPrefix);
}
