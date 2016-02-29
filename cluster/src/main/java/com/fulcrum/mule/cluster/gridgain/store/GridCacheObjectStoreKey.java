package com.fulcrum.mule.cluster.gridgain.store;

import java.io.Serializable;

/**
 * Created on Feb 10, 2015
 *
 * @author Andrey Maryshev
 */
public class GridCacheObjectStoreKey<K extends Serializable> implements Comparable<GridCacheObjectStoreKey>, Serializable {

    public String keyPrefix;
    public K key;

    public GridCacheObjectStoreKey() {
    }

    public GridCacheObjectStoreKey(String keyPrefix, K key) {
        this.keyPrefix = keyPrefix;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GridCacheObjectStoreKey that = (GridCacheObjectStoreKey) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (keyPrefix != null ? !keyPrefix.equals(that.keyPrefix) : that.keyPrefix != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = keyPrefix != null ? keyPrefix.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(GridCacheObjectStoreKey o) {
        if (key instanceof Comparable && o.key instanceof Comparable) {
            return ((Comparable) key).compareTo((Comparable) o.key);
        }
        return 0;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("GridCacheObjectStoreKey{");
        sb.append("keyPrefix='").append(keyPrefix).append('\'');
        sb.append(", key=").append(key);
        sb.append('}');
        return sb.toString();
    }
}
