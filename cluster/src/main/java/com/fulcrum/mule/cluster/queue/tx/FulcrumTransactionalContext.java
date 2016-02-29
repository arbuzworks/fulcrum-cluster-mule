package com.fulcrum.mule.cluster.queue.tx;

import org.mule.util.xa.ResourceManagerException;

import javax.transaction.xa.XAResource;

/**
 * Created on Feb 24, 2015
 *
 * @author Andrey Maryshev
 */
public interface FulcrumTransactionalContext extends XAResource {

    void begin() throws ResourceManagerException;

    void commit() throws ResourceManagerException;

    void rollback() throws ResourceManagerException;

    XAResource getXaResource();

}
