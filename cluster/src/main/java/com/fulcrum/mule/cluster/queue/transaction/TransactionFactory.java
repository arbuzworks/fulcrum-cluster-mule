/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.queue.transaction;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public interface TransactionFactory
{
    TransactionResource newTransactionalContext(TransactionType txType);
}
