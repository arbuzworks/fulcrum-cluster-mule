/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.boot;

import com.fulcrum.mule.cluster.Cluster;
import com.fulcrum.mule.cluster.ClusterContext;
import com.fulcrum.mule.cluster.config.ClusterConfig;
import com.fulcrum.mule.cluster.exception.ClusterException;
import com.fulcrum.mule.cluster.gridgain.ClusterImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.MuleCoreExtension;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.MuleRuntimeException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.registry.RegistrationException;
import org.mule.module.launcher.AbstractDeploymentListener;

import java.util.HashMap;

/**
 * Created by arbuzworks on 3/12/15.
 **/
public class ClusterExtension extends AbstractDeploymentListener implements MuleCoreExtension
{
    private final Log logger = LogFactory.getLog(getClass());

    private boolean initialized = false;
    private Cluster cluster;

    @Override
    public synchronized void dispose()
    {
        if (cluster != null)
        {
            synchronized (cluster.getClusterContexts())
            {
                for (ClusterContext clusterContext : cluster.getClusterContexts().values())
                {
                    clusterContext.close();
                    clusterContext.dispose();
                }
                cluster.getClusterContexts().clear();
            }

            if (cluster != null)
            {
                try
                {
                    cluster.dispose();
                }
                catch (ClusterException ce)
                {
                    logger.warn("Failed to dispose cluster instance");
                }
                finally
                {
                    cluster = null;
                }
            }

            cluster = null;
        }

        initialized = false;
    }

    @Override
    public synchronized void initialise() throws InitialisationException
    {
        if (!initialized)
        {
            try
            {
                ClusterConfig clusterConfig = new ClusterConfig();
                clusterConfig.initialise();

                cluster = new ClusterImpl();
                cluster.initialise(clusterConfig);
                initialized = true;
            }
            catch (Exception e)
            {
                dispose();
                throw new InitialisationException(e, this);
            }
        }
    }

    @Override
    public String getName()
    {
        return "Fulcrum Clustering Extension";
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
    public void onMuleContextCreated(String artifactName, MuleContext context)
    {
        if (!initialized)
        {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }

        ClusterContext clusterContext = new ClusterContext(artifactName, context);

        try
        {
            clusterContext.initialise(cluster);
            cluster.getClusterContexts().put(artifactName, clusterContext);
        }
        catch (RegistrationException e)
        {
            clusterContext.dispose();
            throw new MuleRuntimeException(e);
        }
    }

    @Override
    public synchronized void onUndeploymentStart(String artifactName)
    {
        if (!initialized)
        {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }

        HashMap<String, ClusterContext> clusterContexts = cluster.getClusterContexts();

        synchronized (clusterContexts)
        {
            for (ClusterContext clusterContext : clusterContexts.values())
            {
                clusterContext.close();
            }
        }
    }

    @Override
    public void onUndeploymentSuccess(String artifactName)
    {
        disposeClusterSupport(artifactName);
    }

    @Override
    public void onUndeploymentFailure(String artifactName, Throwable cause)
    {
        disposeClusterSupport(artifactName);
    }

    private void disposeClusterSupport(String artifactName)
    {
        if (!initialized)
        {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }

        HashMap<String, ClusterContext> clusterContexts = cluster.getClusterContexts();

        synchronized (clusterContexts)
        {
            ClusterContext clusterContext = clusterContexts.remove(artifactName);
            if (clusterContext != null)
            {
                clusterContext.dispose();
            }
        }
    }

}
