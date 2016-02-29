package com.fulcrum.mule.cluster.config;

import org.mule.api.config.MuleProperties;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.config.ClusterConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created on Feb 23, 2015
 *
 * @author Andrey Maryshev
 */
public class FulcrumClusterConfiguration implements ClusterConfiguration {

    private String clusterId;
    private int clusterNodeId;
    private Properties clusterConfigurationProperties;

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public int getClusterNodeId() {
        return clusterNodeId;
    }

    public void initialise() throws InitialisationException {
        File configFile = new File(getMuleHomeDirectory() + "/.mule/fulcrum-cluster.properties");
        if (!configFile.exists()) {
            throw new InitialisationException(new IllegalStateException("fulcrum-cluster.properties file not found"), null);
        }

        FileInputStream inputStream = null;
        Properties clusterConfigurationProperties = new Properties();
        try {
            inputStream = new FileInputStream(configFile);
            clusterConfigurationProperties.load(inputStream);
        } catch (IOException e) {
            throw new InitialisationException(e, null);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    //ignored
                }
            }
        }
        clusterId = clusterConfigurationProperties.getProperty(FulcrumClusterProperties.CLUSTER_ID);
        clusterNodeId = Integer.valueOf(clusterConfigurationProperties.getProperty(FulcrumClusterProperties.CLUSTER_NODE_ID));
        this.clusterConfigurationProperties = clusterConfigurationProperties;
    }

    public void dispose() {

    }

    public String getStringProperty(String key) {
        return getStringProperty(key, null);
    }

    public String getStringProperty(String key, String defaultValue) {
        String value = clusterConfigurationProperties.getProperty(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }
        return value;
    }

    public int getIntProperty(String key) {
        return getIntProperty(key, 0);
    }

    public int getIntProperty(String key, int defaultValue) {
        String value = clusterConfigurationProperties.getProperty(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }
        return Integer.valueOf(value);
    }

    public boolean getBooleanProperty(String key) {
        return getBooleanProperty(key, false);
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = clusterConfigurationProperties.getProperty(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }
        return Boolean.valueOf(value);
    }

    private static String getMuleHomeDirectory() {
        return System.getProperty(MuleProperties.MULE_HOME_DIRECTORY_PROPERTY);
    }

}
