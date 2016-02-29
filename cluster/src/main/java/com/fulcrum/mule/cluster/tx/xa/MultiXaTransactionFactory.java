package com.fulcrum.mule.cluster.tx.xa;

import org.mule.api.MuleContext;
import org.mule.api.transaction.ExternalTransactionAwareTransactionFactory;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.config.i18n.CoreMessages;
import org.mule.transaction.ExternalXaTransaction;
import org.mule.transaction.XaTransaction;

import javax.transaction.TransactionManager;

/**
 * Created on Feb 25, 2015
 *
 * @author Andrey Maryshev
 */
public class MultiXaTransactionFactory implements ExternalTransactionAwareTransactionFactory {

    public Transaction beginTransaction(MuleContext muleContext) throws TransactionException {
        try {
            XaTransaction xat = new XaTransaction(muleContext);
            xat.begin();
            return xat;
        } catch (Exception e) {
            throw new TransactionException(CoreMessages.cannotStartTransaction("XA"), e);
        }
    }

    public Transaction joinExternalTransaction(MuleContext muleContext) throws TransactionException {
        try {
            TransactionManager txManager = muleContext.getTransactionManager();
            if (txManager.getTransaction() == null) {
                return null;
            }
            XaTransaction xat = new ExternalXaTransaction(muleContext);
            xat.begin();
            return xat;
        } catch (Exception e) {
            throw new TransactionException(CoreMessages.cannotStartTransaction("XA"), e);
        }
    }

    public boolean isTransacted() {
        return true;
    }
}
