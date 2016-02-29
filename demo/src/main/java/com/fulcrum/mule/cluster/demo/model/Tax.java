package com.fulcrum.mule.cluster.demo.model;

import java.io.Serializable;

public class Tax implements Serializable {
	private double rate;

	public double getRate() {
		return rate;
	}

	public void setRate(double rate) {
		this.rate = rate;
	}
}
