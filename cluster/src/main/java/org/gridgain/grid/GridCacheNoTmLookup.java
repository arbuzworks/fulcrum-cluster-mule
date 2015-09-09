/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.gridgain.grid;

import org.jetbrains.annotations.Nullable;

import javax.transaction.TransactionManager;

/**
 * Created by arbuzworks on 3/24/15.
 **/
public class GridCacheNoTmLookup implements GridCacheTmLookup
{

    @Nullable
    @Override
    public TransactionManager getTm() throws GridException
    {
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }
}
