package com.fulcrum.mule.cluster.tx;

import org.mule.api.MuleContext;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.api.transaction.TransactionFactory;

/**
 * Created on Feb 25, 2015
 *
 * @author Andrey Maryshev
 */
public class MultiTransactionFactory implements TransactionFactory {

    public static ThreadLocal<Transaction> aggregateTransactions = new ThreadLocal<Transaction>();

    @Override
    public Transaction beginTransaction(MuleContext muleContext) throws TransactionException {
        Transaction transaction = aggregateTransactions.get();
        if (transaction == null || transaction.getStatus() != Transaction.STATUS_ACTIVE) {
            transaction = new MultiResourceTransaction(muleContext);
            transaction.begin();
            aggregateTransactions.set(transaction);
        }
        return transaction;
    }

    @Override
    public boolean isTransacted() {
        return true;
    }

}
