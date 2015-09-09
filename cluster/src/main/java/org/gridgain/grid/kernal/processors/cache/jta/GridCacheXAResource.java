/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.gridgain.grid.kernal.processors.cache.jta;

import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCacheTxState;
import org.gridgain.grid.kernal.processors.cache.GridCacheContext;
import org.gridgain.grid.kernal.processors.cache.GridCacheTxEx;
import org.gridgain.grid.logger.GridLogger;
import org.gridgain.grid.util.typedef.internal.S;
import org.gridgain.grid.util.typedef.internal.U;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.atomic.AtomicReference;

import static org.gridgain.grid.cache.GridCacheTxState.PREPARED;
import static org.gridgain.grid.cache.GridCacheTxState.ACTIVE;
import static org.gridgain.grid.cache.GridCacheTxState.COMMITTED;
import static org.gridgain.grid.cache.GridCacheTxState.ROLLED_BACK;


/**
 * Created by arbuzworks on 3/24/15.
 **/
public final class GridCacheXAResource implements XAResource
{
    /**
     * Logger reference.
     */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** */
    private static final Xid[] NO_XID = new Xid[]{};

    /**
     * Context.
     */
    private GridCacheContext cctx;

    /**
     * Cache transaction.
     */
    private GridCacheTxEx cacheTx;

    /** */
    private GridLogger log;

    /** */
    private Xid xid;

    /**
     * @param cacheTx Cache jta.
     * @param cctx    Cache context.
     */
    public GridCacheXAResource(GridCacheTxEx cacheTx, GridCacheContext cctx)
    {
        assert cacheTx != null;
        assert cctx != null;

        this.cctx = cctx;
        this.cacheTx = cacheTx;

        log = U.logger(cctx.kernalContext(), logRef, GridCacheXAResource.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(Xid xid, int flags)
    {
        if (log.isDebugEnabled())
        {
            log.debug("XA resource start(...) [xid=" + xid + ", flags=<" + flags(flags) + ">]");
        }

        // Simply save global transaction id.
        this.xid = xid;
    }

    /**
     * @param msg   Message.
     * @param cause Cause.
     * @throws javax.transaction.xa.XAException XA exception.
     */
    private void throwException(String msg, Throwable cause) throws XAException
    {
        XAException ex = new XAException(msg);

        ex.initCause(cause);

        throw ex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(Xid xid) throws XAException
    {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
        {
            log.debug("XA resource rollback(...) [xid=" + xid + "]");
        }

        try
        {
            cacheTx.rollback();
        }
        catch (GridException e)
        {
            throwException("Failed to rollback cache transaction: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int prepare(Xid xid) throws XAException
    {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
        {
            log.debug("XA resource prepare(...) [xid=" + xid + "]");
        }
        if (cacheTx.state() != ACTIVE)
        {
            throw new XAException("Cache transaction is not in active state.");
        }
        try
        {
            cacheTx.prepare();
        }
        catch (GridException e)
        {
            throwException("Failed to prepare cache transaction.", e);
        }

        return XA_OK;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end(Xid xid, int flags)
    {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
        {
            log.debug("XA resource end(...) [xid=" + xid + ", flags=<" + flags(flags) + ">]");
        }
        if ((flags & TMFAIL) > 0)
        {
            cacheTx.setRollbackOnly();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException
    {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
        {
            log.debug("XA resource commit(...) [xid=" + xid + ", onePhase=" + onePhase + "]");
        }
        try
        {
            cacheTx.commit();
        }
        catch (GridException e)
        {
            throwException("Failed to commit cache transaction: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forget(Xid xid) throws XAException
    {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
        {
            log.debug("XA resource forget(...) [xid=" + xid + "]");
        }
        try
        {
            cacheTx.invalidate(true);

            cacheTx.commit();
        }
        catch (GridException e)
        {
            throwException("Failed to forget cache transaction: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Xid[] recover(int i)
    {
        if (cacheTx.state() == PREPARED)
        {
            return new Xid[]{xid};
        }

        return NO_XID;
    }

    /**
     * @param flags JTA Flags.
     * @return Comma-separated flags string.
     */
    private String flags(int flags)
    {
        StringBuilder res = new StringBuilder();

        addFlag(res, flags, TMENDRSCAN, "TMENDRSCAN");
        addFlag(res, flags, TMFAIL, "TMFAIL");
        addFlag(res, flags, TMJOIN, "TMJOIN");
        addFlag(res, flags, TMNOFLAGS, "TMNOFLAGS");
        addFlag(res, flags, TMONEPHASE, "TMONEPHASE");
        addFlag(res, flags, TMRESUME, "TMRESUME");
        addFlag(res, flags, TMSTARTRSCAN, "TMSTARTRSCAN");
        addFlag(res, flags, TMSUCCESS, "TMSUCCESS");
        addFlag(res, flags, TMSUSPEND, "TMSUSPEND");

        return res.toString();
    }

    /**
     * @param sb       String builder.
     * @param flags    Flags bit set.
     * @param mask     Bit mask.
     * @param flagName String name of the flag specified by given mask.
     * @return String builder appended by flag if it's presented in bit set.
     */
    private StringBuilder addFlag(StringBuilder sb, int flags, int mask, String flagName)
    {
        if ((flags & mask) > 0)
        {
            sb.append(sb.length() > 0 ? "," : "").append(flagName);
        }

        return sb;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTransactionTimeout()
    {
        return (int) cacheTx.timeout();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSameRM(XAResource xar)
    {
        if (xar == this)
        {
            return true;
        }
        if (!(xar instanceof GridCacheXAResource))
        {
            return false;
        }
        GridCacheXAResource other = (GridCacheXAResource) xar;

        return cctx == other.cctx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setTransactionTimeout(int i)
    {
        cacheTx.timeout(i);

        return true;
    }

    /**
     * @return {@code true} if jta was already committed or rolled back.
     */
    public boolean isFinished()
    {
        GridCacheTxState state = cacheTx.state();

        return state == COMMITTED || state == ROLLED_BACK;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return S.toString(GridCacheXAResource.class, this);
    }
}
