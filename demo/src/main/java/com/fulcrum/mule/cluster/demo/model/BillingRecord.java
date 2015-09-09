/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.demo.model;

import java.io.Serializable;

/**
 * Created by arbuzworks on 3/30/15.
 **/
public class BillingRecord implements Serializable
{

    private String phoneNumber;
    private double amount;
    private int minutes;

    public String getPhoneNumber()
    {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber)
    {
        this.phoneNumber = phoneNumber;
    }

    public double getAmount()
    {
        return amount;
    }

    public void setAmount(double amount)
    {
        this.amount = amount;
    }

    public int getMinutes()
    {
        return minutes;
    }

    public void setMinutes(int minutes)
    {
        this.minutes = minutes;
    }

    @Override
    public String toString()
    {
        return "BillingRecord{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", amount=" + amount +
                ", minutes=" + minutes +
                '}';
    }

}
