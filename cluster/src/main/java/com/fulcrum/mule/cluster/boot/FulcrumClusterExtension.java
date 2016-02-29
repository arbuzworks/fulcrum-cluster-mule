package com.fulcrum.mule.cluster.boot;

import com.fulcrum.mule.cluster.FulcrumClusterManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.MuleCoreExtension;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.MuleRuntimeException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.module.launcher.AbstractDeploymentListener;

/**
 * Created on Feb 23, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterExtension extends AbstractDeploymentListener implements MuleCoreExtension {

    private static final Log LOGGER = LogFactory.getLog(FulcrumClusterExtension.class);

    private boolean initialized = false;
    private FulcrumClusterManager fulcrumClusterManager;

    @Override
    public String getName() {
        return "Fulcrum Cluster Extension";
    }

    @Override
    public synchronized void initialise() throws InitialisationException {
        if (!initialized) {
            try {
                (fulcrumClusterManager = new FulcrumClusterManager()).initialise();
                initialized = true;
            } catch (Exception e) {
                dispose();
                throw new InitialisationException(e, this);
            }
        }
    }

    @Override
    public void start() throws MuleException {
    }

    @Override
    public void stop() throws MuleException {
    }

    @Override
    public synchronized void dispose() {
        try {
            if (fulcrumClusterManager != null) {
                fulcrumClusterManager.dispose();
            }
        } catch (Exception ex) {
            LOGGER.warn("Exception disposing of ClusterManager", ex);
        } finally {
            fulcrumClusterManager = null;
            initialized = false;
        }
    }

    @Override
    public synchronized void onMuleContextCreated(String artifactName, MuleContext context) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }
        fulcrumClusterManager.onContextCreated(artifactName, context);
    }

    @Override
    public void onDeploymentStart(String artifactName) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }
        fulcrumClusterManager.onDeploymentStart(artifactName);
    }

    @Override
    public void onDeploymentFailure(String artifactName, Throwable cause) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized", cause));
        }
        fulcrumClusterManager.onDeploymentFailure(artifactName, cause);
    }

    @Override
    public synchronized void onDeploymentSuccess(String artifactName) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }
        fulcrumClusterManager.onDeploymentSuccess(artifactName);
    }

    @Override
    public synchronized void onUndeploymentStart(String artifactName) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }
        fulcrumClusterManager.onUndeploymentStart(artifactName);
    }

    @Override
    public void onUndeploymentSuccess(String artifactName) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized"));
        }
        fulcrumClusterManager.onUndeploymentSuccess(artifactName);
    }

    @Override
    public void onUndeploymentFailure(String artifactName, Throwable cause) {
        if (!initialized) {
            throw new MuleRuntimeException(new IllegalStateException("ClusterManager not initialized", cause));
        }
        fulcrumClusterManager.onUndeploymentFailure(artifactName, cause);
    }
}
