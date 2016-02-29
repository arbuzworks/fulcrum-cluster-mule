package com.fulcrum.mule.cluster.gridgain;

import com.fulcrum.mule.cluster.FulcrumCluster;
import com.fulcrum.mule.cluster.FulcrumPrimaryPollingInstanceListener;
import com.fulcrum.mule.cluster.config.FulcrumClusterConfiguration;
import com.fulcrum.mule.cluster.config.FulcrumClusterProperties;
import com.fulcrum.mule.cluster.gridgain.queue.GridCacheQueueStore;
import com.fulcrum.mule.cluster.gridgain.queue.tx.GridCacheTransactionFactory;
import com.fulcrum.mule.cluster.gridgain.store.GridCacheObjectStore;
import com.fulcrum.mule.cluster.gridgain.store.GridCacheObjectStoreKey;
import com.fulcrum.mule.cluster.queue.tx.FulcrumTransactionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.mule.api.MuleContext;
import org.mule.api.MuleRuntimeException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.store.ListableObjectStore;
import org.mule.util.queue.QueueConfiguration;
import org.mule.util.queue.QueueStore;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created on Feb 23, 2015
 *
 * @author Andrey Maryshev
 */
public class GridGainCluster implements FulcrumCluster {

    private static final Log LOGGER = LogFactory.getLog(GridGainCluster.class);

    private GridCacheTransactionFactory transactionFactory;
    private final List<FulcrumPrimaryPollingInstanceListener> topologyListeners = new ArrayList<>();
    private Ignite grid;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Override
    public void initialise(FulcrumClusterConfiguration fulcrumClusterConfiguration) throws InitialisationException {
        try {
            IgniteConfiguration cfg = createConfiguration(fulcrumClusterConfiguration);
            HashMap<IgnitePredicate<? extends Event>, int[]> eventListeners = new HashMap<>();
            eventListeners.put(new IgnitePredicate<DiscoveryEvent>() {
                @Override
                public boolean apply(DiscoveryEvent gridDiscoveryEvent) {
                    switch (gridDiscoveryEvent.type()) {
                        case EventType.EVT_NODE_JOINED:
                        case EventType.EVT_NODE_LEFT:
                        case EventType.EVT_NODE_FAILED:
                            checkPrimaryNodeInstance();
                            break;
                    }
                    return true;
                }
            }, EventType.EVTS_DISCOVERY);
            cfg.setLocalEventListeners(eventListeners);
            // Events
            cfg.setIncludeEventTypes(EventType.EVTS_DISCOVERY);
            grid = Ignition.start(cfg);

            transactionFactory = new GridCacheTransactionFactory(grid);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(30000l);
                        checkPrimaryNodeInstance();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
//            scheduler.scheduleAtFixedRate(new Runnable() {
//                @Override
//                public void run() {
//                    if (Boolean.valueOf(String.valueOf(grid.localNode().meta("primary")))) {
//                        GridCache<Object, Object> cache = cache(STORE_CACHE);
//                        try {
//                            LOGGER.error("---------------- REAPER START ---------------------");
//                            LOGGER.error("---------------- REAPER CACHE SIZE: " + cache.keySet().size());
//                            LOGGER.error("---------------- REAPER CACHE PRIMARY SIZE: " + cache.globalPrimarySize());
//                            LOGGER.error("---------------- REAPER CACHE GLOBAL SIZE: " + cache.globalSize());
//                            cache.compactAll();
//                            LOGGER.error("---------------- REAPER END ---------------------");
//                        } catch (GridException e) {
//                            e.printStackTrace(System.err);
//                        }
//                    }
//                }
//            }, 10, 30, TimeUnit.SECONDS);
        } catch (IgniteException e) {
            throw new InitialisationException(e, null);
        }
    }

    private IgniteConfiguration createConfiguration(FulcrumClusterConfiguration config) throws IgniteException {
        IgniteConfiguration cfg = new IgniteConfiguration();
        //cfg.setRestEnabled(false);//TODO find out how to disable REST
        cfg.setLocalHost(config.getStringProperty(FulcrumClusterProperties.CLUSTER_NETWORK_INTERFACES, "127.0.0.1"));
        // Discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder =
                config.getBooleanProperty(FulcrumClusterProperties.CLUSTER_MULTICAST_ENABLED, true)
                        ? new TcpDiscoveryMulticastIpFinder()
                        : new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(
                config.getStringProperty(FulcrumClusterProperties.CLUSTER_NODES, "127.0.0.1:47500..47509")));
        discoSpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoSpi);

        // Marshaller
        cfg.setMarshaller(new OptimizedMarshaller(false));

        CacheConfiguration queueCacheConfig = new CacheConfiguration();
        queueCacheConfig.setName(FulcrumCluster.QUEUE_CACHE);
        queueCacheConfig.setStartSize(1000);
        queueCacheConfig.setTransactionManagerLookupClassName("org.apache.ignite.cache.jta.CacheNoTmLookup");
        queueCacheConfig.setCacheMode(CacheMode.REPLICATED);
        queueCacheConfig.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        queueCacheConfig.setPreloadMode(CachePreloadMode.SYNC);
        queueCacheConfig.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        queueCacheConfig.setMaxConcurrentAsyncOperations(1);
        queueCacheConfig.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);

        CacheConfiguration storeCacheConfig = new CacheConfiguration();
        storeCacheConfig.setName(FulcrumCluster.STORE_CACHE);
        storeCacheConfig.setStartSize(100000);
        storeCacheConfig.setTransactionManagerLookupClassName("org.apache.ignite.cache.jta.CacheNoTmLookup");

        storeCacheConfig.setCacheMode(CacheMode.valueOf(
                config.getStringProperty(FulcrumClusterProperties.STORE_CACHE_MODE, "REPLICATED")));
        storeCacheConfig.setAtomicityMode(CacheAtomicityMode.valueOf(
                config.getStringProperty(FulcrumClusterProperties.STORE_ATOMICITY_MODE, "TRANSACTIONAL")));
        storeCacheConfig.setPreloadMode(CachePreloadMode.SYNC);
        /*storeCacheConfig.setDistributionMode(GridCacheDistributionMode.valueOf(
                config.getStringProperty(FulcrumClusterProperties.STORE_DISTRIBUTION_MODE, "NEAR_PARTITIONED")));*/
        storeCacheConfig.setWriteSynchronizationMode(CacheWriteSynchronizationMode.valueOf(
                config.getStringProperty(FulcrumClusterProperties.STORE_WRITE_SYNCHRONIZATION_MODE, "FULL_SYNC")));
        storeCacheConfig.setMemoryMode(CacheMemoryMode.valueOf(
                config.getStringProperty(FulcrumClusterProperties.STORE_MEMORY_MODE, "OFFHEAP_VALUES")));
        cfg.setCacheConfiguration(queueCacheConfig, storeCacheConfig);

        TransactionConfiguration tc = new TransactionConfiguration();
        tc.setDefaultTxConcurrency(TransactionConcurrency.PESSIMISTIC);
        tc.setDefaultTxIsolation(TransactionIsolation.SERIALIZABLE);
        tc.setTxSerializableEnabled(true);
        cfg.setTransactionConfiguration(tc);

        RoundRobinLoadBalancingSpi loadBalancingSpi = new RoundRobinLoadBalancingSpi();
        loadBalancingSpi.setPerTask(false);
        cfg.setLoadBalancingSpi(loadBalancingSpi);

        cfg.setFailoverSpi(new AlwaysFailoverSpi());

        return cfg;
    }

    private void checkPrimaryNodeInstance() {
        Collection<ClusterNode> nodes = grid.cluster().nodes();
        TreeMap<Long, UUID> nodeCache = new TreeMap<>();
        for (ClusterNode node : nodes) {
            if (Boolean.valueOf(String.valueOf(((TcpDiscoveryNode) node).meta("primary")))) {
                nodeCache.clear();
                nodeCache.put(node.metrics().getNodeStartTime(), node.id());
                break;
            }
            nodeCache.put(node.metrics().getNodeStartTime(), node.id());
        }
        ClusterNode localNode = grid.cluster().localNode();
        if (nodeCache.size() > 0) {
            Map.Entry<Long, UUID> entry = nodeCache.firstEntry();
            if (localNode.id().equals(entry.getValue())) {
                ((TcpDiscoveryNode)localNode).putMetaIfAbsent("primary", "true");
                synchronized (topologyListeners) {
                    for (FulcrumPrimaryPollingInstanceListener listener : topologyListeners) {
                        listener.onPrimaryPollingInstance();
                    }
                }
            }
        }
    }

    @Override
    public void dispose() {
        scheduler.shutdownNow();
        synchronized (topologyListeners) {
            topologyListeners.clear();
        }
        try {
            grid.close();
        } catch (IgniteException e) {
            //ignored
        }
    }

    @Override
    public FulcrumTransactionFactory getTransactionFactory() {
        return transactionFactory;
    }

    @Override
    public void registerClusterTopologyListener(FulcrumPrimaryPollingInstanceListener listener) {
        synchronized (topologyListeners) {
            if (!topologyListeners.contains(listener)) {
                topologyListeners.add(listener);
            }
        }
    }

    @Override
    public QueueStore queue(MuleContext muleContext, String queueName,
                            QueueConfiguration config, boolean collocated) {
        try {
            CollectionConfiguration colCfg = new CollectionConfiguration();
            colCfg.setCacheName(FulcrumCluster.QUEUE_CACHE);
            colCfg.setCollocated(collocated);
            IgniteQueue<Object> queue = grid.queue(resolveQueueName(muleContext, queueName), config.getCapacity(), colCfg);
            return new GridCacheQueueStore(queue, queueName, config);
        } catch (IgniteException e) {
            throw new MuleRuntimeException(e);
        }
    }

    private <K, V> IgniteCache<K, V> cache(String cacheName) {
        return grid.jcache(cacheName);
    }

    @Override
    public <V extends Serializable> ListableObjectStore<V> store(final String keyPrefix) {
        IgniteCache<GridCacheObjectStoreKey<Serializable>, V> cache = cache(STORE_CACHE);
        GridCacheObjectStore<V> keyPrefixObjectStore = new GridCacheObjectStore<V>(keyPrefix, cache);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("---> new Map created: " + keyPrefix);
        }
        return keyPrefixObjectStore;
    }

    public static String resolveQueueName(MuleContext muleContext, String queueName) {
        return resolveQueueName(muleContext.getConfiguration().getId(), queueName);
    }

    public static String resolveQueueName(String appId, String queueName) {
        return getQueuePrefix(appId) + queueName;
    }

    public static String getQueuePrefix(String appId) {
        return appId + QUEUE_LABEL;
    }

}
