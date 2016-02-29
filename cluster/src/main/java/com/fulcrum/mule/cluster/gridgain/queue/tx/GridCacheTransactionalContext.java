package com.fulcrum.mule.cluster.gridgain.queue.tx;

import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionType;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionalContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.internal.processors.cache.jta.CacheJtaManager;
import org.apache.ignite.internal.processors.cache.jta.GridCacheXAResource;
import org.mule.util.xa.ResourceManagerException;

import javax.resource.spi.LocalTransactionException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Created on Feb 24, 2015
 *
 * @author Andrey Maryshev
 */
class GridCacheTransactionalContext implements FulcrumTransactionalContext {

    private Ignite grid;
    private FulcrumTransactionType transactionType;
    private Transaction localResource;
    private GridCacheXAResource xaResource;

    GridCacheTransactionalContext(Ignite grid, FulcrumTransactionType txType) {
        this.grid = grid;
        this.transactionType = txType;
    }

    private void checkLocal() throws ResourceManagerException {
        if (!FulcrumTransactionType.LOCAL.equals(transactionType)) {
            throw new ResourceManagerException(new LocalTransactionException("Type 2Phase can not be applied to local on begin"));
        }
    }

    private void check2Phase() throws XAException {
        if (!FulcrumTransactionType.TWO_PHASE.equals(transactionType)) {
            throw new XAException("Type Local can not be applied to 2Phase on begin");
        }
    }

    @Override
    public void begin() throws ResourceManagerException {
        checkLocal();
        localResource = grid.transactions().txStart();
    }

    @Override
    public void commit() throws ResourceManagerException {
        checkLocal();
        try {
            localResource.commit();
        } catch (Exception e) {
            throw new ResourceManagerException(e);
        } finally {
            localResource = null;
        }
    }

    @Override
    public void rollback() throws ResourceManagerException {
        checkLocal();
        try {
            localResource.rollback();
        } catch (IgniteException e) {
            throw new ResourceManagerException(e);
        }
    }

    @Override
    public XAResource getXaResource() {
        return this;
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        check2Phase();
        xaResource = CacheJtaManager.createXAResource();
        xaResource.start(xid, i);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        check2Phase();
        return xaResource.prepare(xid);
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        check2Phase();
        xaResource.end(xid, flags);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        check2Phase();
        xaResource.forget(xid);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        check2Phase();
        return xaResource.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xar) throws XAException {
        check2Phase();
        return xaResource.isSameRM(xar);
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        check2Phase();
        return xaResource.recover(i);
    }

    @Override
    public void commit(Xid xid, boolean b) throws XAException {
        check2Phase();
        xaResource.commit(xid, b);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        check2Phase();
        xaResource.rollback(xid);
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException {
        check2Phase();
        return false; //not supported
    }

}
