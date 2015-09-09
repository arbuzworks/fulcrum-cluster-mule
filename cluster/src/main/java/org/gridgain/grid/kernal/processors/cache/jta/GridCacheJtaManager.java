/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.gridgain.grid.kernal.processors.cache.jta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gridgain.grid.GridCacheTmLookup;
import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheTx;
import org.gridgain.grid.kernal.processors.cache.GridCacheTxEx;
import org.jetbrains.annotations.Nullable;

import static org.gridgain.grid.cache.GridCacheFlag.INVALIDATE;

/**
 * Created by arbuzworks on 3/24/15.
 **/
public final class GridCacheJtaManager<K, V> extends GridCacheJtaManagerAdapter<K, V>
{

    private final Log logger = LogFactory.getLog(getClass());

    private final ThreadLocal<GridCacheXAResource> xaRsrc = new ThreadLocal<>();
    private GridCacheTmLookup tmLookup;

    private static volatile GridCacheJtaManager gridCacheJtaManager;

    public GridCacheJtaManager()
    {
        synchronized (GridCacheJtaManager.class)
        {
            if (gridCacheJtaManager == null)
            {
                gridCacheJtaManager = this;
            }
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void createTmLookup(GridCacheConfiguration ccfg) throws GridException
    {
        assert ccfg.getTransactionManagerLookupClassName() != null;
        try
        {
            Class<?> cls = Class.forName(ccfg.getTransactionManagerLookupClassName());
            tmLookup = (GridCacheTmLookup) cls.newInstance();
        }
        catch (Exception e)
        {
            throw new GridException("Failed to instantiate transaction manager lookup.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkJta() throws GridException
    {
        logger.warn("checkJta() is ignored");
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public Object tmLookup()
    {
        return tmLookup;
    }

    public static GridCacheXAResource createXAResource()
    {
        return gridCacheJtaManager.findOrCreateXAResource();
    }

    private GridCacheXAResource findOrCreateXAResource()
    {
        GridCacheXAResource rsrc = xaRsrc.get();
        if (rsrc == null || rsrc.isFinished())
        {
            GridCacheTx tx = cctx.tm().userTx();
            if (tx == null)
            {
                // Start with default concurrency and isolation.
                GridCacheConfiguration cfg = cctx.config();
                tx = cctx.tm().onCreated(
                        cctx.cache().newTx(
                                false,
                                false,
                                cfg.getDefaultTxConcurrency(),
                                cfg.getDefaultTxIsolation(),
                                cfg.getDefaultTxTimeout(),
                                cfg.isInvalidate() || cctx.hasFlag(INVALIDATE),
                                cctx.syncCommit(),
                                cctx.syncRollback(),
                                cctx.isSwapOrOffheapEnabled(),
                                cctx.isStoreEnabled(),
                                0,
                                /** group lock keys */null,
                                /** partition lock */false
                        )
                );
            }
            rsrc = new GridCacheXAResource((GridCacheTxEx) tx, cctx);
            xaRsrc.set(rsrc);
        }
        return rsrc;
    }
}
