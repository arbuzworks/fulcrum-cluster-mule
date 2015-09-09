/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.gridgain.queue.transaction;

import com.fulcrum.mule.cluster.queue.transaction.TransactionFactory;
import com.fulcrum.mule.cluster.queue.transaction.TransactionResource;
import com.fulcrum.mule.cluster.queue.transaction.TransactionType;
import org.gridgain.grid.cache.GridCache;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class TransactionFactoryImpl implements TransactionFactory
{
    private GridCache gridCache;

    public TransactionFactoryImpl(GridCache gridCache)
    {
        this.gridCache = gridCache;
    }

    @Override
    public TransactionResource newTransactionalContext(TransactionType transactionType)
    {
        return new TransactionResourceImpl(gridCache, transactionType);
    }
}
