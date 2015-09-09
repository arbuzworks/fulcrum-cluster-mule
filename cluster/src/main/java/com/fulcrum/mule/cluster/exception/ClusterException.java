/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.exception;

import com.fulcrum.mule.cluster.Cluster;
import org.mule.api.MuleException;
import org.mule.config.i18n.CoreMessages;
import org.mule.config.i18n.Message;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class ClusterException extends MuleException
{
    private transient Cluster cluster;

    public ClusterException(Message message, Cluster cluster)
    {
        super(message);
        this.cluster = cluster;
    }

    public ClusterException(Message message, Throwable cause, Cluster cluster)
    {
        super(message, cause);
        this.cluster = cluster;
    }

    public ClusterException(Throwable cause, Cluster cluster)
    {
        super(CoreMessages.createStaticMessage(cause.getMessage()), cause);
        this.cluster = cluster;
        this.addInfo("Cluster", cluster.toString());
    }

    public Object getCluster()
    {
        return this.cluster;
    }
}
