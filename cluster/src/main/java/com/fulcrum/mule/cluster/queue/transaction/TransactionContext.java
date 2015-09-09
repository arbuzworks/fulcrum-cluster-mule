/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.queue.transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.util.queue.QueueStore;
import org.mule.util.queue.QueueTransactionContext;

import java.io.Serializable;

/**
 * Created by arbuzworks on 3/23/15.
 **/
public final class TransactionContext implements QueueTransactionContext
{
    private final Log logger = LogFactory.getLog(getClass());

    public static final int LONG_TIMEOUT = 10000;
    public static final int SHORT_TIMEOUT = 10;

    private static TransactionContext transactionContext = null;

    private TransactionContext()
    {
    }

    public static TransactionContext getInstance()
    {
        if (transactionContext == null)
        {
            transactionContext = new TransactionContext();
        }
        return transactionContext;
    }

    @Override
    public boolean offer(QueueStore queue, Serializable item, long offerTimeout) throws InterruptedException
    {
        return queue.offer(item, -1, offerTimeout);
    }

    @Override
    public void untake(QueueStore queue, Serializable item) throws InterruptedException
    {
        offer(queue, item, LONG_TIMEOUT);
    }

    @Override
    public void clear(QueueStore queue) throws InterruptedException
    {
        while (poll(queue, SHORT_TIMEOUT) != null)
        {
            logger.info("Cleared queue store");
        }
    }

    @Override
    public Serializable poll(QueueStore queue, long pollTimeout) throws InterruptedException
    {
        return queue.poll(pollTimeout);
    }

    @Override
    public Serializable peek(QueueStore queue) throws InterruptedException
    {
        return queue.peek();
    }

    @Override
    public int size(QueueStore queue)
    {
        return queue.getSize();
    }
}
