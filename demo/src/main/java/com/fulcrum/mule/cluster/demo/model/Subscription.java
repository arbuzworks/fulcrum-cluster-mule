package com.fulcrum.mule.cluster.demo.model;

import java.io.Serializable;

public class Subscription implements Serializable {
	private int minutes;
	private double rate;

	public int getMinutes() {
		return minutes;
	}

	public void setMinutes(int minutes) {
		this.minutes = minutes;
	}

	public double getRate() {
		return rate;
	}

	public void setRate(double rate) {
		this.rate = rate;
	}
}
