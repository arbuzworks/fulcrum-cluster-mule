package com.fulcrum.mule.cluster.queue;

import com.fulcrum.mule.cluster.queue.tx.FulcrumQueueTransactionContext;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionFactory;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionType;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionalContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.util.queue.AbstractQueueSession;
import org.mule.util.queue.QueueSession;
import org.mule.util.queue.QueueTransactionContext;
import org.mule.util.xa.ResourceManagerException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Created on Feb 10, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterQueueSession extends AbstractQueueSession implements QueueSession {

    private transient Log logger = LogFactory.getLog(getClass());

    private FulcrumQueueTransactionContext queueTransactionContext;
    private FulcrumTransactionFactory transactionFactory;
    private FulcrumTransactionalContext transactionalContext;
    private Integer xaTransactionTimeout;

    public FulcrumClusterQueueSession(FulcrumClusterQueueManager fulcrumClusterQueueManager, MuleContext muleContext) {
        super(fulcrumClusterQueueManager, muleContext);
        transactionFactory = fulcrumClusterQueueManager.getApplicationContext().transactionFactory();
    }

    @Override
    protected QueueTransactionContext getTransactionalContext() {
        return queueTransactionContext;
    }

    @Override
    public void begin() throws ResourceManagerException {
        try {
            transactionalContext = transactionFactory.newTransactionalContext(FulcrumTransactionType.LOCAL);
            transactionalContext.begin();
            queueTransactionContext = FulcrumQueueTransactionContext.INSTANCE;
            if (logger.isTraceEnabled()) {
                logger.warn(">>> Local Tx begin");
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> Local Tx begin failed", e);
            }
            throw e;
        }
    }

    @Override
    public void commit() throws ResourceManagerException {
        try {
            transactionalContext.commit();
            transactionalContext = null;
            queueTransactionContext = null;
            if (logger.isTraceEnabled()) {
                logger.warn(">>> Local Tx commit");
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> Local Tx commit failed", e);
            }
            throw e;
        }
    }

    @Override
    public void rollback() throws ResourceManagerException {
        try {
            transactionalContext.rollback();
            transactionalContext = null;
            queueTransactionContext = null;
            if (logger.isTraceEnabled()) {
                logger.warn(">>> Local Tx rollback");
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> Local Tx rollback failed", e);
            }
            throw e;
        }
    }

    private FulcrumTransactionalContext createXaTransactionContext() {
        return transactionFactory.newTransactionalContext(FulcrumTransactionType.TWO_PHASE);
    }

    // ---------------------------------------
    // ----- XA Resource implementation ------
    // ---------------------------------------
    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        try {
            if ((xaResource instanceof FulcrumClusterQueueSession)) {
                xaResource = ((FulcrumClusterQueueSession) xaResource).transactionalContext.getXaResource();
            }
            boolean sameRM = transactionalContext.getXaResource().isSameRM(xaResource);
            if (logger.isTraceEnabled()) {
                logger.warn(">>> isSameRM " + sameRM);
            }
            return sameRM;
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> isSameRM failed", e);
            }
            throw e;
        }
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        try {
            int transactionTimeout = transactionalContext.getXaResource().getTransactionTimeout();
            if (logger.isTraceEnabled()) {
                logger.warn(">>> getTransactionTimeout");
            }
            return transactionTimeout;
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> getTransactionTimeout failed", e);
            }
            throw e;
        }
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException {
        xaTransactionTimeout = timeout;
        return true;
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        try {
            transactionalContext = createXaTransactionContext();
            transactionalContext.getXaResource().start(xid, i);
            if (xaTransactionTimeout != null) {
                transactionalContext.getXaResource().setTransactionTimeout(xaTransactionTimeout);
                xaTransactionTimeout = null;
            }
            queueTransactionContext = FulcrumQueueTransactionContext.INSTANCE;
            if (logger.isTraceEnabled()) {
                logger.warn(">>> start: " + xid);
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> start failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        try {
            int prepare = transactionalContext.getXaResource().prepare(xid);
            if (logger.isTraceEnabled()) {
                logger.warn(">>> prepare: " + xid);
            }
            return prepare;
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> prepare failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        try {
            transactionalContext.getXaResource().forget(xid);
            transactionalContext = null;
            queueTransactionContext = null;
            if (logger.isTraceEnabled()) {
                logger.warn(">>> start: " + xid);
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> start failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public void end(Xid xid, int flag) throws XAException {
        try {
            transactionalContext.getXaResource().end(xid, flag);
            if (logger.isTraceEnabled()) {
                logger.warn(">>> forget: " + xid);
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> forget failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Recovery called on cluster session with flag " + flag);
            }
            transactionalContext = createXaTransactionContext();
            Xid[] recover = transactionalContext.getXaResource().recover(flag);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Cluster queue session recover return %s dangling transactions", (recover != null ? recover.length : 0)));
            }
            if (logger.isTraceEnabled()) {
                logger.warn(">>> recover");
            }
            return recover;
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> recover failed", e);
            }
            throw e;
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        try {
            transactionalContext.getXaResource().rollback(xid);
            transactionalContext = null;
            queueTransactionContext = null;
            if (logger.isTraceEnabled()) {
                logger.warn(">>> rollback: " + xid);
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> rollback failed: " + xid, e);
            }
            throw e;
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        try {
            if (queueTransactionContext == null) {
                createXaTransactionContext().getXaResource().commit(xid, onePhase);
            } else {
                transactionalContext.getXaResource().commit(xid, onePhase);
                transactionalContext = null;
                queueTransactionContext = null;
            }
            if (logger.isTraceEnabled()) {
                logger.warn(">>> commit: " + xid);
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.warn(">>> commit failed: " + xid, e);
            }
            throw e;
        }
    }
}
