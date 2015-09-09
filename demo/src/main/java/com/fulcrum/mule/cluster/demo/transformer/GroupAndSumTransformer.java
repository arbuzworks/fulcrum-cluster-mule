/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.demo.transformer;

import com.fulcrum.mule.cluster.demo.model.BillingRecord;
import com.fulcrum.mule.cluster.demo.model.Invoice;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class GroupAndSumTransformer extends AbstractMessageTransformer
{
    @SuppressWarnings("unchecked")
    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException
    {
        CopyOnWriteArrayList<BillingRecord> payload = (CopyOnWriteArrayList<BillingRecord>) message.getPayload();
        Map<String, Invoice> result = new HashMap<>();

        for (BillingRecord record : payload)
        {
            String phoneNumber = record.getPhoneNumber();
            int minutes = record.getMinutes();
            double amount = record.getAmount();

            if (result.containsKey(phoneNumber))
            {
                result.get(phoneNumber).addMinutes(minutes);
                result.get(phoneNumber).addTotal(amount);

            }
            else
            {
                result.put(phoneNumber, new Invoice(minutes, amount));
            }
        }

        return result;
    }
}
