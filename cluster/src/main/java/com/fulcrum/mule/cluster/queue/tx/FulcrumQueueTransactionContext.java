package com.fulcrum.mule.cluster.queue.tx;

import org.mule.util.queue.QueueStore;
import org.mule.util.queue.QueueTransactionContext;

import java.io.Serializable;

/**
 * Created on Feb 10, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumQueueTransactionContext implements QueueTransactionContext {

    public static final int LONG_TIMEOUT = 10000;
    public static final int SHORT_TIMEOUT = 10;

    public static final FulcrumQueueTransactionContext INSTANCE = new FulcrumQueueTransactionContext();

    private FulcrumQueueTransactionContext() {
    }

    @Override
    public boolean offer(QueueStore queue, Serializable item, long offerTimeout) throws InterruptedException {
        return queue.offer(item, -1, offerTimeout);
    }

    @Override
    public void untake(QueueStore queue, Serializable item) throws InterruptedException {
        offer(queue, item, LONG_TIMEOUT);
    }

    @Override
    public void clear(QueueStore queue) throws InterruptedException {
        while (poll(queue, SHORT_TIMEOUT) != null) {
        }
    }

    @Override
    public Serializable poll(QueueStore queue, long pollTimeout) throws InterruptedException {
        return queue.poll(pollTimeout);
    }

    @Override
    public Serializable peek(QueueStore queue) throws InterruptedException {
        return queue.peek();
    }

    @Override
    public int size(QueueStore queue) {
        return queue.getSize();
    }
}
