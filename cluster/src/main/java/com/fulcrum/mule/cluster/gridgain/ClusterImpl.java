/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.gridgain;

import com.fulcrum.mule.cluster.Cluster;
import com.fulcrum.mule.cluster.ClusterContext;
import com.fulcrum.mule.cluster.PrimaryPollingInstanceListener;
import com.fulcrum.mule.cluster.config.ClusterConfig;
import com.fulcrum.mule.cluster.config.ClusterProperties;
import com.fulcrum.mule.cluster.exception.ClusterException;
import com.fulcrum.mule.cluster.gridgain.queue.QueueStoreImpl;
import com.fulcrum.mule.cluster.gridgain.queue.transaction.TransactionFactoryImpl;
import com.fulcrum.mule.cluster.gridgain.store.ObjectStoreImpl;
import com.fulcrum.mule.cluster.gridgain.store.ObjectStoreKey;
import com.fulcrum.mule.cluster.queue.transaction.TransactionFactory;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.GridNode;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheAtomicityMode;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheMemoryMode;
import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.cache.GridCachePreloadMode;
import org.gridgain.grid.cache.GridCacheTxConcurrency;
import org.gridgain.grid.cache.GridCacheTxIsolation;
import org.gridgain.grid.cache.GridCacheWriteSynchronizationMode;
import org.gridgain.grid.cache.datastructures.GridCacheQueue;
import org.gridgain.grid.events.GridDiscoveryEvent;
import org.gridgain.grid.events.GridEvent;
import org.gridgain.grid.events.GridEventType;
import org.gridgain.grid.lang.GridPredicate;
import org.gridgain.grid.marshaller.optimized.GridOptimizedMarshaller;
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast.GridTcpDiscoveryMulticastIpFinder;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder;
import org.gridgain.grid.spi.failover.always.GridAlwaysFailoverSpi;
import org.gridgain.grid.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingSpi;
import org.mule.api.MuleContext;
import org.mule.api.MuleRuntimeException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.store.ListableObjectStore;
import org.mule.context.notification.ClusterNodeNotification;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class ClusterImpl implements Cluster
{

    private Grid grid;
    private final Set<PrimaryPollingInstanceListener> topologyListeners = new HashSet<>();
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private TransactionFactory transactionFactory;
    private ClusterConfig clusterConfig;
    private final AtomicBoolean primaryPollingInstance = new AtomicBoolean(false);

    private final HashMap<String, ClusterContext> clusterContexts = new HashMap<>();

    public HashMap<String, ClusterContext> getClusterContexts()
    {
        return clusterContexts;
    }

    @Override
    public synchronized void initialise(ClusterConfig clusterConfig) throws InitialisationException
    {
        try
        {
            this.clusterConfig = clusterConfig;
            GridConfiguration gridConfiguration = createConfiguration();
            HashMap<GridPredicate<? extends GridEvent>, int[]> eventListeners = new HashMap<>();
            eventListeners.put(new GridPredicate<GridDiscoveryEvent>()
            {
                @Override
                public boolean apply(GridDiscoveryEvent gridDiscoveryEvent)
                {
                    switch (gridDiscoveryEvent.type())
                    {
                        case GridEventType.EVT_NODE_JOINED:
                        case GridEventType.EVT_NODE_LEFT:
                        case GridEventType.EVT_NODE_FAILED:
                            checkPrimaryNodeInstance();
                            break;
                        default:
                    }
                    return true;
                }
            }, GridEventType.EVTS_DISCOVERY);
            gridConfiguration.setLocalEventListeners(eventListeners);
            gridConfiguration.setIncludeEventTypes(GridEventType.EVTS_DISCOVERY);

            grid = GridGain.start(gridConfiguration);

            topologyListeners.add(this);

            transactionFactory = new TransactionFactoryImpl(grid.cache(Cluster.QUEUE_CACHE));

            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.sleep(30000L);
                        checkPrimaryNodeInstance();
                    }
                    catch (InterruptedException e)
                    {
                        //TODO
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        catch (GridException e)
        {
            throw new InitialisationException(e, null);
        }
    }

    @Override
    public ClusterConfig getConfiguration()
    {
        return clusterConfig;
    }

    @Override
    public void dispose() throws ClusterException
    {
        scheduler.shutdownNow();
        synchronized (topologyListeners)
        {
            topologyListeners.clear();
        }
        try
        {
            grid.close();
        }
        catch (GridException e)
        {
            throw new ClusterException(e, this);
        }
    }

    @Override
    public TransactionFactory getTransactionFactory()
    {
        return transactionFactory;
    }

    @Override
    public QueueStore getQueueStore(MuleContext muleContext, String queueName, QueueConfiguration config, boolean collocated, boolean create)
    {
        try
        {
            GridCacheQueue<Object> queue = grid.cache(Cluster.QUEUE_CACHE).dataStructures()
                    .queue(resolveQueueName(muleContext, queueName), config.getCapacity(), collocated, create);
            return new QueueStoreImpl(muleContext, queue, queueName, config);
        }
        catch (GridException e)
        {
            throw new MuleRuntimeException(e);
        }
    }

    @Override
    public <V extends Serializable> ListableObjectStore<V> getObjectStore(MuleContext muleContext, String keyPrefix)
    {
        GridCache<ObjectStoreKey<Serializable>, byte[]> cache = grid.cache(Cluster.STORE_CACHE);
        ObjectStoreImpl<V> keyPrefixObjectStore = new ObjectStoreImpl<V>(muleContext, keyPrefix, cache);

        return keyPrefixObjectStore;
    }

    private void checkPrimaryNodeInstance()
    {
        Collection<GridNode> nodes = grid.nodes();
        TreeMap<Long, UUID> nodeCache = new TreeMap<>();
        for (GridNode node : nodes)
        {
            if (Boolean.valueOf(String.valueOf(node.meta("primary"))))
            {
                nodeCache.clear();
                nodeCache.put(node.metrics().getNodeStartTime(), node.id());
                break;
            }
            nodeCache.put(node.metrics().getNodeStartTime(), node.id());
        }
        GridNode localNode = grid.localNode();
        if (nodeCache.size() > 0)
        {
            Map.Entry<Long, UUID> entry = nodeCache.firstEntry();
            if (localNode.id().equals(entry.getValue()))
            {
                localNode.putMetaIfAbsent("primary", "true");
                synchronized (topologyListeners)
                {
                    for (PrimaryPollingInstanceListener listener : topologyListeners)
                    {
                        listener.onPrimaryPollingInstance();
                    }
                }
            }
        }
    }

    @Override
    public void onPrimaryPollingInstance()
    {
        synchronized (primaryPollingInstance)
        {
            if (primaryPollingInstance.get())
            {
                return;
            }
            primaryPollingInstance.set(true);
        }

        HashMap<String, ClusterContext> clusterContexts = getClusterContexts();

        synchronized (clusterContexts)
        {
            for (ClusterContext clusterContext : clusterContexts.values())
            {
                clusterContext.fireNotification(new ClusterNodeNotification("Becoming primary node", ClusterNodeNotification.PRIMARY_CLUSTER_NODE_SELECTED));
            }
        }
    }

    @Override
    public boolean isPrimaryPollingInstance()
    {
        synchronized (primaryPollingInstance)
        {
            return primaryPollingInstance.get();
        }
    }

    private GridConfiguration createConfiguration() throws GridException
    {
        GridConfiguration gridConfiguration = new GridConfiguration();

        gridConfiguration.setRestEnabled(false);
        gridConfiguration.setLocalHost(clusterConfig.getStringProperty(ClusterProperties.CLUSTER_NETWORK_INTERFACES, "127.0.0.1"));

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();
        GridTcpDiscoveryVmIpFinder ipFinder =
                clusterConfig.getBooleanProperty(ClusterProperties.CLUSTER_MULTICAST_ENABLED, true)
                        ? new GridTcpDiscoveryMulticastIpFinder()
                        : new GridTcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(
                clusterConfig.getStringProperty(ClusterProperties.CLUSTER_NODES, "127.0.0.1:47500..47509")));
        discoSpi.setIpFinder(ipFinder);
        gridConfiguration.setDiscoverySpi(discoSpi);

        gridConfiguration.setMarshaller(new GridOptimizedMarshaller(false));

        GridCacheConfiguration queueCacheConfig = new GridCacheConfiguration();
        queueCacheConfig.setName(Cluster.QUEUE_CACHE);
        queueCacheConfig.setStartSize(1000);
        queueCacheConfig.setTransactionManagerLookupClassName("org.gridgain.grid.GridCacheNoTmLookup");
        queueCacheConfig.setCacheMode(GridCacheMode.REPLICATED);
        queueCacheConfig.setAtomicityMode(GridCacheAtomicityMode.TRANSACTIONAL);
        queueCacheConfig.setPreloadMode(GridCachePreloadMode.SYNC);
        queueCacheConfig.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        queueCacheConfig.setMemoryMode(GridCacheMemoryMode.OFFHEAP_VALUES);

        GridCacheConfiguration storeCacheConfig = new GridCacheConfiguration();
        storeCacheConfig.setName(Cluster.STORE_CACHE);
        storeCacheConfig.setStartSize(100000);
        storeCacheConfig.setTransactionManagerLookupClassName("org.gridgain.grid.GridCacheNoTmLookup");
        storeCacheConfig.setCacheMode(GridCacheMode.valueOf(clusterConfig.getStringProperty(ClusterProperties.STORE_CACHE_MODE, "REPLICATED")));
        storeCacheConfig.setAtomicityMode(GridCacheAtomicityMode.valueOf(clusterConfig.getStringProperty(ClusterProperties.STORE_ATOMICITY_MODE, "TRANSACTIONAL")));
        storeCacheConfig.setPreloadMode(GridCachePreloadMode.SYNC);
        storeCacheConfig.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.valueOf(clusterConfig.getStringProperty(ClusterProperties.STORE_WRITE_SYNCHRONIZATION_MODE, "FULL_SYNC")));
        storeCacheConfig.setMemoryMode(GridCacheMemoryMode.valueOf(clusterConfig.getStringProperty(ClusterProperties.STORE_MEMORY_MODE, "OFFHEAP_VALUES")));
        storeCacheConfig.setDefaultTxConcurrency(GridCacheTxConcurrency.PESSIMISTIC);
        storeCacheConfig.setDefaultTxIsolation(GridCacheTxIsolation.SERIALIZABLE);
        storeCacheConfig.setTxSerializableEnabled(true);

        gridConfiguration.setCacheConfiguration(queueCacheConfig, storeCacheConfig);

        GridRoundRobinLoadBalancingSpi loadBalancingSpi = new GridRoundRobinLoadBalancingSpi();
        loadBalancingSpi.setPerTask(false);
        gridConfiguration.setLoadBalancingSpi(loadBalancingSpi);

        gridConfiguration.setFailoverSpi(new GridAlwaysFailoverSpi());

        return gridConfiguration;
    }

    private static String resolveQueueName(MuleContext muleContext, String queueName)
    {
        return muleContext.getConfiguration().getId() + Cluster.QUEUE_LABEL + queueName;
    }

    @Override
    public String toString()
    {
        return "ClusterImpl{" +
                "grid=" + grid.name() +
                '}';
    }
}
