/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.gridgain.queue;

import org.gridgain.grid.cache.datastructures.GridCacheQueue;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.MuleRuntimeException;
import org.mule.api.lifecycle.Startable;
import org.mule.api.lifecycle.Stoppable;
import org.mule.api.transformer.Transformer;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.simple.ByteArrayToObject;
import org.mule.transformer.simple.ObjectToByteArray;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by arbuzworks on 3/16/15.
 **/
public class QueueStoreImpl implements QueueStore, Startable, Stoppable
{

    private final Transformer serializer;
    private final Transformer deserializer;

    private GridCacheQueue<Object> queue;
    private final String name;
    private final QueueConfiguration config;

    private final AtomicBoolean started = new AtomicBoolean(true);

    public QueueStoreImpl(MuleContext muleContext, GridCacheQueue<Object> queue, String name, QueueConfiguration config)
    {
        this.serializer = new ObjectToByteArray();
        this.serializer.setMuleContext(muleContext);
        this.deserializer = new ByteArrayToObject();
        this.deserializer.setMuleContext(muleContext);
        this.queue = queue;
        this.config = config;
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void putNow(Serializable o) throws InterruptedException
    {
        try
        {
            queue.put(serializer.transform(o));
        }
        catch (TransformerException e)
        {
            throw new MuleRuntimeException(e);
        }
    }

    @Override
    public boolean offer(Serializable o, int room, long timeout) throws InterruptedException
    {
        try
        {
            return queue.offer(serializer.transform(o), timeout, TimeUnit.MILLISECONDS);
        }
        catch (TransformerException e)
        {
            throw new MuleRuntimeException(e);
        }
    }

    @Override
    public Serializable poll(long timeout) throws InterruptedException
    {
        synchronized (started)
        {
            if (started.get())
            {
                Object poll = queue.poll(timeout, TimeUnit.MILLISECONDS);
                if (poll != null)
                {
                    try
                    {
                        poll = deserializer.transform(poll);
                    }
                    catch (TransformerException e)
                    {
                        throw new MuleRuntimeException(e);
                    }
                }
                return (Serializable) poll;
            }
            Thread.sleep(timeout);
            return null;
        }
    }

    @Override
    public Serializable peek() throws InterruptedException
    {
        synchronized (started)
        {
            if (started.get())
            {
                Object peek = queue.peek();
                if (peek != null)
                {
                    try
                    {
                        peek = deserializer.transform(peek);
                    }
                    catch (TransformerException e)
                    {
                        throw new MuleRuntimeException(e);
                    }
                }
                return (Serializable) peek;
            }
            return null;
        }
    }

    @Override
    public void untake(Serializable item) throws InterruptedException
    {
        try
        {
            queue.put(serializer.transform(item));
        }
        catch (TransformerException e)
        {
            throw new MuleRuntimeException(e);
        }
    }

    @Override
    public int getSize()
    {
        synchronized (started)
        {
            if (started.get())
            {
                return queue.size();
            }
            return 0;
        }
    }

    @Override
    public void clear() throws InterruptedException
    {
        queue.clear();
    }

    @Override
    public int getCapacity()
    {
        return config.getCapacity();
    }

    @Override
    public void close()
    {
        synchronized (started)
        {
            started.set(false);
        }
    }

    @Override
    public boolean isPersistent()
    {
        return false;
    }

    @Override
    public void start() throws MuleException
    {
    }

    @Override
    public void stop() throws MuleException
    {
    }

    @Override
    public void dispose()
    {
        queue = null;
    }

}
