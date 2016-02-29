package com.fulcrum.mule.cluster.queue.tx;

/**
 * Created on Feb 24, 2015
 *
 * @author Andrey Maryshev
 */
public interface FulcrumTransactionFactory {

    FulcrumTransactionalContext newTransactionalContext(FulcrumTransactionType txType);

}
