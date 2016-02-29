package com.fulcrum.mule.cluster.demo.model;

import java.io.Serializable;

public class Fee implements Serializable {
	private double amount;

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}
}
