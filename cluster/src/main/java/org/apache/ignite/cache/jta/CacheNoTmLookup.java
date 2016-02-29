package org.apache.ignite.cache.jta;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

import javax.transaction.TransactionManager;

/**
 * Created on Feb 18, 2015
 *
 * @author Andrey Maryshev
 */
public class CacheNoTmLookup implements CacheTmLookup {

    @Nullable
    @Override
    public TransactionManager getTm() throws IgniteException {
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }
}
