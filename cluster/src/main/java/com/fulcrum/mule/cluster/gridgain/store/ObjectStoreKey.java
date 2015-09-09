/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.gridgain.store;

import java.io.Serializable;

/**
 * Created by arbuzworks on 3/16/15.
 **/
public class ObjectStoreKey<K extends Serializable> implements Comparable<ObjectStoreKey>, Serializable
{

    private String keyPrefix;
    private K key;

    public ObjectStoreKey(String keyPrefix, K key)
    {
        this.keyPrefix = keyPrefix;
        this.key = key;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        ObjectStoreKey that = (ObjectStoreKey) o;

        if (key != null ? !key.equals(that.key) : that.key != null)
        {
            return false;
        }

        if (keyPrefix != null ? !keyPrefix.equals(that.keyPrefix) : that.keyPrefix != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = keyPrefix != null ? keyPrefix.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(ObjectStoreKey o)
    {
        if (key instanceof Comparable && o.key instanceof Comparable)
        {
            return ((Comparable) key).compareTo((Comparable) o.key);
        }
        return 0;
    }

    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer("ClusterObjectStoreKey{");
        sb.append("keyPrefix='").append(keyPrefix).append('\'');
        sb.append(", key=").append(key);
        sb.append('}');
        return sb.toString();
    }

    public String getKeyPrefix()
    {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix)
    {
        this.keyPrefix = keyPrefix;
    }

    public K getKey()
    {
        return key;
    }

    public void setKey(K key)
    {
        this.key = key;
    }
}

