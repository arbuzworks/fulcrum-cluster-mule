/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.queue;

import com.fulcrum.mule.cluster.ClusterContext;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.util.queue.AbstractQueueManager;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueSession;
import org.mule.util.queue.QueueStore;
import org.mule.util.queue.RecoverableQueueStore;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class ClusterQueueManager extends AbstractQueueManager
{
    private ClusterContext clusterContext;

    public ClusterQueueManager(ClusterContext clusterContext)
    {
        this.clusterContext = clusterContext;
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    @Override
    public QueueSession getQueueSession()
    {
        return new ClusterQueueSession(this, getMuleContext());
    }

    @Override
    public void initialise() throws InitialisationException
    {

    }

    @Override
    public void start() throws MuleException
    {
    }

    @Override
    public void stop() throws MuleException
    {
    }

    @Override
    protected void doDispose()
    {
        clusterContext = null;
    }

    @Override
    protected QueueStore createQueueStore(String queueName, QueueConfiguration config)
    {
        return clusterContext.getDistributedQueueStore(queueName, config, true, true);
    }

    @Override
    public RecoverableQueueStore getRecoveryQueue(String queueName)
    {
        throw new UnsupportedOperationException("Recovery queues are not available for cluster nor required");
    }

}
