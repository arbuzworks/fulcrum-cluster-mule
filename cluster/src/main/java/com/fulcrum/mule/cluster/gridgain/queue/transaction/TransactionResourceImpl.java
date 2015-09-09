/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.gridgain.queue.transaction;

import com.fulcrum.mule.cluster.queue.transaction.TransactionResource;
import com.fulcrum.mule.cluster.queue.transaction.TransactionType;
import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheTx;
import org.gridgain.grid.kernal.processors.cache.jta.GridCacheJtaManager;
import org.gridgain.grid.kernal.processors.cache.jta.GridCacheXAResource;
import org.mule.util.xa.ResourceManagerException;

import javax.resource.spi.LocalTransactionException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Created by arbuzworks on 3/23/15.
 **/
public class TransactionResourceImpl implements TransactionResource
{

    private GridCache gridCache;
    private TransactionType transactionType;
    private GridCacheTx localResource;
    private GridCacheXAResource xaResource;

    public TransactionResourceImpl(GridCache gridCache, TransactionType transactionType)
    {
        this.gridCache = gridCache;
        this.transactionType = transactionType;
    }

    private void checkLocal() throws ResourceManagerException
    {
        if (!TransactionType.LOCAL.equals(transactionType))
        {
            throw new ResourceManagerException(new LocalTransactionException("Type 2Phase can not be applied to local on begin"));
        }
    }

    private void check2Phase() throws XAException
    {
        if (!TransactionType.TWO_PHASE.equals(transactionType))
        {
            throw new XAException("Type Local can not be applied to 2Phase on begin");
        }
    }

    @Override
    public void begin() throws ResourceManagerException
    {
        checkLocal();
        localResource = gridCache.txStart();
    }

    @Override
    public void commit() throws ResourceManagerException
    {
        checkLocal();
        try
        {
            localResource.commit();
        }
        catch (Exception e)
        {
            throw new ResourceManagerException(e);
        }
        finally
        {
            localResource = null;
        }
    }

    @Override
    public void rollback() throws ResourceManagerException
    {
        checkLocal();
        try
        {
            localResource.rollback();
        }
        catch (GridException e)
        {
            throw new ResourceManagerException(e);
        }
    }

    @Override
    public XAResource getXaResource()
    {
        return this;
    }

    @Override
    public void start(Xid xid, int i) throws XAException
    {
        check2Phase();
        xaResource = GridCacheJtaManager.createXAResource();
        xaResource.start(xid, i);
    }

    @Override
    public int prepare(Xid xid) throws XAException
    {
        check2Phase();
        return xaResource.prepare(xid);
    }

    @Override
    public void end(Xid xid, int flags) throws XAException
    {
        check2Phase();
        xaResource.end(xid, flags);
    }

    @Override
    public void forget(Xid xid) throws XAException
    {
        check2Phase();
        xaResource.forget(xid);
    }

    @Override
    public int getTransactionTimeout() throws XAException
    {
        check2Phase();
        return xaResource.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xar) throws XAException
    {
        check2Phase();
        return xaResource.isSameRM(xar);
    }

    @Override
    public Xid[] recover(int i) throws XAException
    {
        check2Phase();
        return xaResource.recover(i);
    }

    @Override
    public void commit(Xid xid, boolean b) throws XAException
    {
        check2Phase();
        xaResource.commit(xid, b);
    }

    @Override
    public void rollback(Xid xid) throws XAException
    {
        check2Phase();
        xaResource.rollback(xid);
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException
    {
        check2Phase();
        return false;
    }
}
