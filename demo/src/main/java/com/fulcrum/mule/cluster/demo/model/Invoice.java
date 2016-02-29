package com.fulcrum.mule.cluster.demo.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * Created on Feb 18, 2015
 *
 * @author Andrey Maryshev
 */
public class Invoice implements Serializable {

    public int minutes;
    public Subscription subscription;
    public Fee fee;
    public Tax tax;

    public double total;

    public Invoice() {
    }

    public Invoice(int minutes) {
        this.minutes = minutes;
    }

    public int getMinutes() {
        return minutes;
    }

    public void setMinutes(int minutes) {
        this.minutes = minutes;
    }

    public Invoice addMinutes(int minutes) {
        this.minutes += minutes;
        return this;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }

    public Fee getFee() {
        return fee;
    }

    public void setFee(Fee fee) {
        this.fee = fee;
    }

    public Tax getTax() {
        return tax;
    }

    public void setTax(Tax tax) {
        this.tax = tax;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public void calculate() {
        int value = minutes;
        if (value > subscription.getMinutes()) {
            total = (subscription.getRate() + ((value - subscription.getMinutes()) * fee.getAmount())) +
                    ((subscription.getRate() + ((value - subscription.getMinutes()) * fee.getAmount())) * tax.getRate());
        } else {
            total = subscription.getRate() + (subscription.getRate() * tax.getRate());
        }
    }

    public String getTotalAsString() {
        return NumberFormat.getCurrencyInstance(Locale.US).format((new BigDecimal(total)).setScale(2, RoundingMode.HALF_UP).doubleValue());
    }
}
