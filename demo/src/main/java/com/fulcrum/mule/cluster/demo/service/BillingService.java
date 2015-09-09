/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.demo.service;

import com.fulcrum.mule.cluster.demo.model.BillingRecord;
import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

/**
 * Created by arbuzworks on 3/30/15.
 **/
public class BillingService implements Callable
{
    @Override
    public Object onCall(MuleEventContext muleEventContext) throws Exception
    {

        String payload = (String) muleEventContext.getMessage().getPayload();

        String[] columns = payload.split("[,]");
        
        BillingRecord billingRecord = new BillingRecord();

        billingRecord.setPhoneNumber(columns[0]);
        billingRecord.setMinutes(Integer.parseInt(columns[1]));
        billingRecord.setAmount(Integer.parseInt(columns[1]) * 0.10);

        return billingRecord;
    }
}
