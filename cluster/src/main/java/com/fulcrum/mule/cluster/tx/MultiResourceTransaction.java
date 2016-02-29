package com.fulcrum.mule.cluster.tx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.api.transaction.TransactionFactory;
import org.mule.api.transaction.UnboundTransactionFactory;
import org.mule.transaction.AbstractSingleResourceTransaction;
import org.mule.transaction.TransactionCollection;
import org.mule.transport.jms.JmsTransactionFactory;
import org.mule.transport.vm.VMTransactionFactory;
import org.mule.util.queue.QueueManager;

import javax.jms.Connection;
import java.util.*;

/**
 * Created on Feb 25, 2015
 *
 * @author Andrey Maryshev
 */
public class MultiResourceTransaction extends AbstractSingleResourceTransaction implements TransactionCollection {

    private static final Log LOGGER = LogFactory.getLog(MultiResourceTransaction.class);

    private static Map<Class, Class<? extends TransactionFactory>> resource2Transaction;

    static {
        Map<Class, Class<? extends TransactionFactory>> classMappings = new HashMap<>(2);
        classMappings.put(Connection.class, JmsTransactionFactory.class);
        classMappings.put(QueueManager.class, VMTransactionFactory.class);
        resource2Transaction = Collections.unmodifiableMap(classMappings);
    }

    private final List<Transaction> txCollection = new LinkedList<>();

    public MultiResourceTransaction(MuleContext muleContext) {
        super(muleContext);
    }

    @Override
    protected void doBegin() throws TransactionException {
    }

    @Override
    protected synchronized void doCommit() throws TransactionException {
        LinkedList<Transaction> txReversed = new LinkedList<>(txCollection);
        Collections.reverse(txReversed);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Committing a transaction collection in the following order: " + txReversed);
        }
        MultiTransactionException multiTxEx = null;
        for (Transaction aTxReversed : txReversed) {
            Transaction singleResourceTx = null;
            boolean committing = true;
            try {
                singleResourceTx = aTxReversed;
                if (isRollbackOnly()) {
                    committing = false;
                    singleResourceTx.rollback();
                } else {
                    singleResourceTx.commit();
                }
            } catch (TransactionException e) {
                setRollbackOnly();
                LOGGER.error(String.format("Failed to %s TX: %s", (committing ? "commit" : "resolve"), singleResourceTx), e);
                if (multiTxEx == null) {
                    multiTxEx = new MultiTransactionException(e);
                } else {
                    multiTxEx.addException(e);
                }
            }
        }
        if (multiTxEx != null) {
            throw multiTxEx;
        }
        txCollection.clear();
        committed.set(true);
    }

    @Override
    protected synchronized void doRollback() throws TransactionException {
        List<Transaction> txReversed = new LinkedList<>(txCollection);
        Collections.reverse(txReversed);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Rolling back a transaction collection in the following order: " + txReversed);
        }
        for (Transaction aTxReversed : txReversed) {
            try {
                aTxReversed.rollback();
            } catch (Exception e) {
                LOGGER.error("Failed to rollback TX: " + aTxReversed, e);
            }
        }
        txCollection.clear();
        rolledBack.set(true);
    }

    @Override
    public synchronized void bindResource(Object key, Object resource) throws TransactionException {
        Class<? extends TransactionFactory> txClassFound = findTxClass(key);
        if (txClassFound == null) {
            throw new IllegalArgumentException("Resource " + key + " not supported in this scenario");
        }
        Transaction tx;
        try {
            UnboundTransactionFactory factory = (UnboundTransactionFactory) txClassFound.newInstance();
            tx = factory.createUnboundTransaction(muleContext);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't create transaction from factory " + e);
        }
        tx.bindResource(key, resource);
        tx.begin();
        txCollection.add(tx);
    }

    @Override
    public synchronized Object getResource(Object key) {
        for (Transaction tx : txCollection) {
            if (tx.hasResource(key)) {
                return tx.getResource(key);
            }
        }
        return null;
    }

    @Override
    public synchronized boolean hasResource(Object key) {
        for (Transaction tx : txCollection) {
            if (tx.hasResource(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized void setRollbackOnly() {
        for (Transaction tx : txCollection) {
            try {
                tx.setRollbackOnly();
            } catch (TransactionException ex) {
                LOGGER.error("Failed to mark a TX as rollback-only. TX: " + tx);
            }
        }
        rollbackOnly.set(true);
    }

    @Override
    public synchronized void aggregate(AbstractSingleResourceTransaction tx) {
        txCollection.add(tx);
    }

    @Override
    public synchronized List<Transaction> getTxCollection() {
        return Collections.unmodifiableList(txCollection);
    }

    @Override
    public boolean isRollbackOnly() throws TransactionException {
        for (Transaction tx : txCollection) {
            if (tx.isRollbackOnly()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean supports(Object key, Object resource) {
        return findTxClass(key) != null;
    }

    private Class<? extends TransactionFactory> findTxClass(Object key) {
        for (Class<?> keyClass : resource2Transaction.keySet()) {
            if (keyClass.isAssignableFrom(key.getClass())) {
                return resource2Transaction.get(keyClass);
            }
        }
        return null;
    }

}
