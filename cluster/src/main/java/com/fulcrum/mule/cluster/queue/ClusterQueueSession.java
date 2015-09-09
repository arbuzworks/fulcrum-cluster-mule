/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.queue;

import com.fulcrum.mule.cluster.queue.transaction.TransactionContext;
import com.fulcrum.mule.cluster.queue.transaction.TransactionFactory;
import com.fulcrum.mule.cluster.queue.transaction.TransactionResource;
import com.fulcrum.mule.cluster.queue.transaction.TransactionType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.util.queue.AbstractQueueSession;
import org.mule.util.queue.QueueSession;
import org.mule.util.xa.ResourceManagerException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class ClusterQueueSession extends AbstractQueueSession implements QueueSession
{

    private transient Log logger = LogFactory.getLog(getClass());

    private TransactionContext transactionContext;
    private TransactionFactory transactionFactory;
    private TransactionResource transactionResource;
    private Integer xaTransactionTimeout;

    public ClusterQueueSession(ClusterQueueManager clusterQueueManager, MuleContext muleContext)
    {
        super(clusterQueueManager, muleContext);
        transactionFactory = clusterQueueManager.getClusterContext().getTransactionFactory();
    }

    @Override
    protected TransactionContext getTransactionalContext()
    {
        return transactionContext;
    }

    @Override
    public void begin() throws ResourceManagerException
    {
        try
        {
            transactionResource = transactionFactory.newTransactionalContext(TransactionType.LOCAL);
            transactionResource.begin();
            transactionContext = TransactionContext.getInstance();
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> Local Tx begin");
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> Local Tx begin failed", e);
            }
            throw e;
        }
    }

    @Override
    public void commit() throws ResourceManagerException
    {
        try
        {
            transactionResource.commit();
            transactionResource = null;
            transactionContext = null;
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> Local Tx commit");
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> Local Tx commit failed", e);
            }
            throw e;
        }
    }

    @Override
    public void rollback() throws ResourceManagerException
    {
        try
        {
            transactionResource.rollback();
            transactionResource = null;
            transactionContext = null;
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> Local Tx rollback");
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> Local Tx rollback failed", e);
            }
            throw e;
        }
    }

    private TransactionResource createXaTransactionResource()
    {
        return transactionFactory.newTransactionalContext(TransactionType.TWO_PHASE);
    }

    // ---------------------------------------
    // ----- XA Resource implementation ------
    // ---------------------------------------
    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException
    {
        try
        {
            if ((xaResource instanceof ClusterQueueSession))
            {
                xaResource = ((ClusterQueueSession) xaResource).transactionResource.getXaResource();
            }
            boolean sameRM = transactionResource.getXaResource().isSameRM(xaResource);
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> isSameRM " + sameRM);
            }
            return sameRM;
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> isSameRM failed", e);
            }
            throw e;
        }
    }

    @Override
    public int getTransactionTimeout() throws XAException
    {
        try
        {
            int transactionTimeout = transactionResource.getXaResource().getTransactionTimeout();
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> getTransactionTimeout");
            }
            return transactionTimeout;
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> getTransactionTimeout failed", e);
            }
            throw e;
        }
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException
    {
        xaTransactionTimeout = timeout;
        return true;
    }

    @Override
    public void start(Xid xid, int i) throws XAException
    {
        try
        {
            transactionResource = createXaTransactionResource();
            transactionResource.getXaResource().start(xid, i);
            if (xaTransactionTimeout != null)
            {
                transactionResource.getXaResource().setTransactionTimeout(xaTransactionTimeout);
                xaTransactionTimeout = null;
            }
            transactionContext = TransactionContext.getInstance();
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> start: " + xid);
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> start failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException
    {
        try
        {
            int prepare = transactionResource.getXaResource().prepare(xid);
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> prepare: " + xid);
            }
            return prepare;
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> prepare failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public void forget(Xid xid) throws XAException
    {
        try
        {
            transactionResource.getXaResource().forget(xid);
            transactionResource = null;
            transactionContext = null;
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> start: " + xid);
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> start failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public void end(Xid xid, int flag) throws XAException
    {
        try
        {
            transactionResource.getXaResource().end(xid, flag);
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> forget: " + xid);
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> forget failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public Xid[] recover(int flag) throws XAException
    {
        try
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Recovery called on cluster session with flag " + flag);
            }
            transactionResource = createXaTransactionResource();
            Xid[] recover = transactionResource.getXaResource().recover(flag);
            if (logger.isDebugEnabled())
            {
                logger.debug(String.format("Cluster queue session recover return %s dangling transactions", (recover != null ? recover.length : 0)));
            }
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> recover");
            }
            return recover;
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> recover failed", e);
            }
            throw e;
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException
    {
        try
        {
            transactionResource.getXaResource().rollback(xid);
            transactionResource = null;
            transactionContext = null;
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> rollback: " + xid);
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> rollback failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException
    {
        try
        {
            if (transactionResource == null)
            {
                createXaTransactionResource().getXaResource().commit(xid, onePhase);
            }
            else
            {
                transactionResource.getXaResource().commit(xid, onePhase);
                transactionResource = null;
                transactionContext = null;
            }
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> commit: " + xid);
            }
        }
        catch (Exception e)
        {
            if (logger.isTraceEnabled())
            {
                logger.warn(">>> commit failed: " + xid, e);
            }
            throw e;
        }
    }
}
