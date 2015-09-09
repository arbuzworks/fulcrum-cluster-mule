/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.config;

import org.mule.api.config.MuleProperties;

/**
 * Created by arbuzworks on 3/13/15.
 **/
public class ClusterProperties extends MuleProperties
{
    public static final String MULE_CLUSTER_MANAGER = "_muleClusterManager";

    public static final String CLUSTER_ID = "fulcrum.clusterId";
    public static final String CLUSTER_NODE_ID = "fulcrum.clusterNodeId";
    public static final String CLUSTER_NETWORK_INTERFACES = "fulcrum.cluster.networkinterfaces";
    public static final String CLUSTER_NODES = "fulcrum.cluster.nodes";
    public static final String CLUSTER_MULTICAST_ENABLED = "fulcrum.cluster.multicastenabled";

    public static final String STORE_CACHE_MODE = "fulcrum.objectstore.cacheMode";
    public static final String STORE_ATOMICITY_MODE = "fulcrum.objectstore.atomicityMode";
    public static final String STORE_WRITE_SYNCHRONIZATION_MODE = "fulcrum.objectstore.writeSynchronizationMode";
    public static final String STORE_MEMORY_MODE = "fulcrum.objectstore.memoryMode";
}
