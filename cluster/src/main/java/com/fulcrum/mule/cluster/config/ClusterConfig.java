/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.config;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.config.MuleProperties;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.config.ClusterConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by arbuzworks on 3/13/15.
 **/
public class ClusterConfig implements ClusterConfiguration
{
    private final Log logger = LogFactory.getLog(getClass());

    private String clusterId;
    private int clusterNodeId;
    private Properties clusterProperties;

    @Override
    public String getClusterId()
    {
        return clusterId;
    }

    @Override
    public int getClusterNodeId()
    {
        return clusterNodeId;
    }

    public void initialise() throws InitialisationException
    {
        File configFile = new File(getMuleHomeDirectory() + "/conf/fulcrum-cluster.properties");
        if (!configFile.exists())
        {
            throw new InitialisationException(new IllegalStateException("fulcrum-cluster.properties file not found"), null);
        }

        FileInputStream inputStream = null;
        Properties clusterProperties = new Properties();
        try
        {
            inputStream = new FileInputStream(configFile);
            clusterProperties.load(inputStream);
        }
        catch (IOException e)
        {
            throw new InitialisationException(e, null);
        }
        finally
        {
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    logger.warn("Failed to close input stream");
                }
            }
        }
        clusterId = clusterProperties.getProperty(ClusterProperties.CLUSTER_ID);
        clusterNodeId = Integer.valueOf(clusterProperties.getProperty(ClusterProperties.CLUSTER_NODE_ID));
        this.clusterProperties = clusterProperties;
    }

    public void dispose()
    {

    }

    public String getStringProperty(String key)
    {
        return getStringProperty(key, null);
    }

    public String getStringProperty(String key, String defaultValue)
    {
        String value = clusterProperties.getProperty(key);
        if (value == null || value.length() == 0)
        {
            return defaultValue;
        }
        return value;
    }

    public int getIntProperty(String key)
    {
        return getIntProperty(key, 0);
    }

    public int getIntProperty(String key, int defaultValue)
    {
        String value = clusterProperties.getProperty(key);
        if (value == null || value.length() == 0)
        {
            return defaultValue;
        }
        return Integer.valueOf(value);
    }

    public boolean getBooleanProperty(String key)
    {
        return getBooleanProperty(key, false);
    }

    public boolean getBooleanProperty(String key, boolean defaultValue)
    {
        String value = clusterProperties.getProperty(key);
        if (value == null || value.length() == 0)
        {
            return defaultValue;
        }
        return Boolean.valueOf(value);
    }

    private static String getMuleHomeDirectory()
    {
        return System.getProperty(MuleProperties.MULE_HOME_DIRECTORY_PROPERTY);
    }
}

