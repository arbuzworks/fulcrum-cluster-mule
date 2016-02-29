package com.fulcrum.mule.cluster;

import com.fulcrum.mule.cluster.boot.FulcrumClusterExtension;
import com.fulcrum.mule.cluster.config.FulcrumClusterConfiguration;
import com.fulcrum.mule.cluster.context.FulcrumClusterApplicationContext;
import com.fulcrum.mule.cluster.gridgain.GridGainCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.api.MuleRuntimeException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.registry.RegistrationException;
import org.mule.context.notification.ClusterNodeNotification;
import org.mule.transport.PollingController;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on Feb 23, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterManager implements FulcrumPrimaryPollingInstanceListener, PollingController {

    private static final Log LOGGER = LogFactory.getLog(FulcrumClusterExtension.class);

    private final AtomicBoolean primaryPollingInstance = new AtomicBoolean(false);
    private final HashMap<String, FulcrumClusterApplicationContext> fulcrumClusterApplicationContexts = new HashMap<>();

    FulcrumClusterConfiguration fulcrumClusterConfiguration;
    FulcrumCluster fulcrumCluster;

    public synchronized void initialise() throws InitialisationException {
        fulcrumClusterConfiguration = new FulcrumClusterConfiguration();
        fulcrumClusterConfiguration.initialise();

        fulcrumCluster = new GridGainCluster();
        fulcrumCluster.registerClusterTopologyListener(this);
        fulcrumCluster.initialise(fulcrumClusterConfiguration);
    }

    public synchronized void dispose() {
        synchronized (fulcrumClusterApplicationContexts) {
            for (FulcrumClusterApplicationContext applicationContext : fulcrumClusterApplicationContexts.values()) {
                applicationContext.preDispose();
                applicationContext.dispose();
            }
            fulcrumClusterApplicationContexts.clear();
        }

        if (fulcrumCluster != null) {
            fulcrumCluster.dispose();
            fulcrumCluster = null;
        }

        if (fulcrumClusterConfiguration != null) {
            fulcrumClusterConfiguration.dispose();
            fulcrumClusterConfiguration = null;
        }
    }

    public void onContextCreated(String artifactName, MuleContext context) {
        FulcrumClusterApplicationContext applicationContext = new FulcrumClusterApplicationContext(artifactName, context);
        try {
            applicationContext.initialise(this);
            fulcrumClusterApplicationContexts.put(artifactName, applicationContext);
        } catch (RegistrationException e) {
            applicationContext.dispose();
            throw new MuleRuntimeException(e);
        }
    }

    public void onDeploymentStart(String artifactName) {
    }

    public void onDeploymentFailure(String artifactName, Throwable cause) {
    }

    public void onDeploymentSuccess(String artifactName) {
    }

    public void onUndeploymentStart(String artifactName) {
        synchronized (fulcrumClusterApplicationContexts) {
            for (FulcrumClusterApplicationContext applicationContext : fulcrumClusterApplicationContexts.values()) {
                applicationContext.preDispose();
            }
        }
    }

    public void onUndeploymentSuccess(String artifactName) {
        disposeClusterSupport(artifactName);
    }

    public void onUndeploymentFailure(String artifactName, Throwable cause) {
        disposeClusterSupport(artifactName);
    }

    private void disposeClusterSupport(String artifactName) {
        synchronized (fulcrumClusterApplicationContexts) {
            FulcrumClusterApplicationContext applicationContext = fulcrumClusterApplicationContexts.remove(artifactName);
            if (applicationContext != null) {
                applicationContext.dispose();
            }
        }
    }

    public FulcrumClusterConfiguration getFulcrumClusterConfiguration() {
        return fulcrumClusterConfiguration;
    }

    public FulcrumCluster getCluster() {
        return fulcrumCluster;
    }

    @Override
    public boolean isPrimaryPollingInstance() {
        synchronized (primaryPollingInstance) {
            return primaryPollingInstance.get();
        }
    }

    @Override
    public void onPrimaryPollingInstance() {
        synchronized (primaryPollingInstance) {
            if (primaryPollingInstance.get()) {
                return;
            }
            primaryPollingInstance.set(true);
        }
        synchronized (fulcrumClusterApplicationContexts) {
            for (FulcrumClusterApplicationContext applicationContext : fulcrumClusterApplicationContexts.values()) {
                applicationContext.fireNotification(new ClusterNodeNotification("Becoming primary node", ClusterNodeNotification.PRIMARY_CLUSTER_NODE_SELECTED));
            }
        }
    }

}
