package com.fulcrum.mule.cluster.gridgain.queue;

import org.apache.ignite.IgniteQueue;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.Startable;
import org.mule.api.lifecycle.Stoppable;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on Feb 10, 2015
 *
 * @author Andrey Maryshev
 */
public class GridCacheQueueStore implements QueueStore, Startable, Stoppable {

    private IgniteQueue<Object> queue;
    private final String name;
    private final QueueConfiguration config;

    private final AtomicBoolean started = new AtomicBoolean(true);

    public GridCacheQueueStore(IgniteQueue<Object> queue, String name, QueueConfiguration config) {
        this.queue = queue;
        this.config = config;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void putNow(Serializable o) throws InterruptedException {
        queue.put(o);
    }

    @Override
    public boolean offer(Serializable o, int room, long timeout) throws InterruptedException {
        return queue.offer(o, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Serializable poll(long timeout) throws InterruptedException {
        synchronized (started) {
            if (started.get()) {
                return (Serializable) queue.poll(timeout, TimeUnit.MILLISECONDS);
            }
            Thread.sleep(timeout);
            return null;
        }
    }

    @Override
    public Serializable peek() throws InterruptedException {
        synchronized (started) {
            if (started.get()) {
                return (Serializable) queue.peek();
            }
            return null;
        }
    }

    @Override
    public void untake(Serializable item) throws InterruptedException {
        queue.put(item);
    }

    @Override
    public int getSize() {
        synchronized (started) {
            if (started.get()) {
                return queue.size();
            }
            return 0;
        }
    }

    @Override
    public void clear() throws InterruptedException {
        queue.clear();
    }

    @Override
    public int getCapacity() {
        return config.getCapacity();
    }

    @Override
    public void close() {
        synchronized (started) {
            started.set(false);
        }
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public void start() throws MuleException {
    }

    @Override
    public void stop() throws MuleException {
    }

    @Override
    public void dispose() {
        queue = null;
    }

}
