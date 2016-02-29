package com.fulcrum.mule.cluster.gridgain.queue.tx;

import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionFactory;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionType;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionalContext;
import org.apache.ignite.Ignite;

/**
 * Created on Feb 24, 2015
 *
 * @author Andrey Maryshev
 */
public class GridCacheTransactionFactory implements FulcrumTransactionFactory {

    private Ignite grid;

    public GridCacheTransactionFactory(Ignite grid) {
        this.grid = grid;
    }

    @Override
    public FulcrumTransactionalContext newTransactionalContext(FulcrumTransactionType txType) {
        return new GridCacheTransactionalContext(grid, txType);
    }

}
