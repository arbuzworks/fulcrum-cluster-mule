package com.fulcrum.mule.cluster.queue;

import com.fulcrum.mule.cluster.context.FulcrumClusterApplicationContext;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.util.queue.*;

/**
 * Created on Feb 05, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterQueueManager extends AbstractQueueManager {

    private FulcrumClusterApplicationContext applicationContext;

    public FulcrumClusterQueueManager(FulcrumClusterApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public FulcrumClusterApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public QueueSession getQueueSession() {
        return new FulcrumClusterQueueSession(this, getMuleContext());
    }

    @Override
    public void initialise() throws InitialisationException {
    }

    @Override
    public void start() throws MuleException {
    }

    @Override
    public void stop() throws MuleException {
    }

    @Override
    protected void doDispose() {
        applicationContext = null;
    }

    @Override
    protected QueueStore createQueueStore(String queueName, QueueConfiguration config) {
        /*if (queueName.startsWith("seda.")) {
            return new DefaultQueueStore(queueName, getMuleContext(), config);
        }*/
        return applicationContext.distributedQueue(queueName, config, true);
    }

    @Override
    public RecoverableQueueStore getRecoveryQueue(String queueName) {
        throw new UnsupportedOperationException("Recovery queues are not available for cluster nor required");
    }

}
