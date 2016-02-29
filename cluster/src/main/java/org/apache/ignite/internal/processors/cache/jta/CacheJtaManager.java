package org.apache.ignite.internal.processors.cache.jta;

import com.fulcrum.mule.cluster.boot.FulcrumClusterExtension;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Created on Feb 18, 2015
 *
 * @author Andrey Maryshev
 */
public class CacheJtaManager<K, V> extends CacheJtaManagerAdapter<K, V> {

    private final ThreadLocal<GridCacheXAResource> xaRsrc = new ThreadLocal<>();
    private TransactionManager jtaTm;
    private CacheTmLookup tmLookup;

    private static volatile CacheJtaManager INSTANCE;

    public CacheJtaManager() {
        synchronized (FulcrumClusterExtension.class) {
            if (INSTANCE == null) {
                INSTANCE = this;
            }
        }
    }

    public void createTmLookup(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg.getTransactionManagerLookupClassName() != null;

        try {
            Class e = Class.forName(ccfg.getTransactionManagerLookupClassName());
            this.tmLookup = (CacheTmLookup)e.newInstance();
        } catch (Exception var3) {
            throw new IgniteCheckedException("Failed to instantiate transaction manager lookup.", var3);
        }
    }

    public void checkJta() throws IgniteCheckedException {
        if(this.jtaTm == null) {
            try {
                this.jtaTm = this.tmLookup.getTm();
            } catch (Exception var7) {
                throw new IgniteCheckedException("Failed to get transaction manager: " + var7, var7);
            }
        }

        if(this.jtaTm != null) {
            GridCacheXAResource rsrc = (GridCacheXAResource)this.xaRsrc.get();
            if(rsrc == null || rsrc.isFinished()) {
                try {
                    Transaction e = this.jtaTm.getTransaction();
                    if(e != null) {
                        Object tx = this.cctx.tm().userTx();
                        if(tx == null) {
                            TransactionConfiguration tCfg = this.cctx.kernalContext().config().getTransactionConfiguration();
                            tx = this.cctx.tm().newTx(false, false, false, tCfg.getDefaultTxConcurrency(), tCfg.getDefaultTxIsolation(), tCfg.getDefaultTxTimeout(), false, true, 0, (IgniteTxKey)null, false);
                        }

                        rsrc = new GridCacheXAResource((IgniteInternalTx)tx, this.cctx);

                        this.xaRsrc.set(rsrc);
                    }
                } catch (SystemException var5) {
                    throw new IgniteCheckedException("Failed to obtain JTA transaction.", var5);
                }
            }
        }

    }

    @Nullable
    public Object tmLookup() {
        return this.tmLookup;
    }

    public static GridCacheXAResource createXAResource() {
        return INSTANCE.findOrCreateXAResource();
    }

    private GridCacheXAResource findOrCreateXAResource() {
        GridCacheXAResource rsrc = xaRsrc.get();
        if (rsrc == null || rsrc.isFinished()) {
            IgniteInternalTx tx = cctx.tm().userTx();
            if (tx == null) {
                TransactionConfiguration tCfg = cctx.kernalContext().config().getTransactionConfiguration();
                tx = cctx.tm().newTx(
                                /*implicit*/false,
                                /*implicit single*/false,
                                /*system*/false,
                                tCfg.getDefaultTxConcurrency(),
                                tCfg.getDefaultTxIsolation(),
                                tCfg.getDefaultTxTimeout(),
                                /*invalidate*/false,
                                /*store enabled*/true,
                                /*tx size*/0,
                                /*group lock keys*/null,
                                /*partition lock*/false
                );
            }
            rsrc = new GridCacheXAResource(tx, cctx);
            xaRsrc.set(rsrc);
        }
        return rsrc;
    }
}
