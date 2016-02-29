package com.fulcrum.mule.cluster.tx;

import org.mule.api.transaction.TransactionException;
import org.mule.config.i18n.Message;

import java.util.LinkedList;
import java.util.List;

/**
 * Created on Feb 25, 2015
 *
 * @author Andrey Maryshev
 */
public class MultiTransactionException extends TransactionException {

    private List<TransactionException> txExceptions = new LinkedList<>();

    public MultiTransactionException(Throwable cause) {
        super(cause);
        validateExceptionType(cause);
        txExceptions.add((TransactionException) cause);
    }

    public MultiTransactionException(Message message) {
        super(message);
    }

    public MultiTransactionException(Message message, Throwable cause) {
        super(message, cause);
        validateExceptionType(cause);
        txExceptions.add((TransactionException) cause);
    }

    protected void validateExceptionType(Throwable cause) {
        if (!(cause instanceof TransactionException)) {
            throw new IllegalArgumentException("You must pass in a TransactionException as an argument");
        }
    }

    public void addException(TransactionException txe) {
        txExceptions.add(txe);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MultiTransactionException");
        sb.append("{txExceptions=").append(txExceptions);
        sb.append('}');
        return sb.toString();
    }
}
