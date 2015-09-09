/*
 * Copyright (c) 2015. Arbuz LLC.  All rights reserved.  http://arbuzworks.com/
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.fulcrum.mule.cluster.demo.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * Created by arbuzworks on 3/18/15.
 **/
public class Invoice implements Serializable
{

    private int minutes;
    private double total;


    public Invoice(int minutes, double total)
    {
        this.minutes = minutes;
        this.total = total;
    }

    public int getMinutes()
    {
        return minutes;
    }

    public void setMinutes(int minutes)
    {
        this.minutes = minutes;
    }

    public Invoice addMinutes(int minutes)
    {
        this.minutes += minutes;
        return this;
    }
    
    public Invoice addTotal(double total)
    {
        this.total += total;
        return this;
    }
    
    public double getTotal()
    {
        return total;
    }

    public void setTotal(double total)
    {
        this.total = total;
    }

    public String getTotalAsString()
    {
        return NumberFormat.getCurrencyInstance(Locale.US).format((new BigDecimal(total)).setScale(2, RoundingMode.HALF_UP).doubleValue());
    }
}
