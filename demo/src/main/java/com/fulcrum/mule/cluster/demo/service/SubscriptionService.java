package com.fulcrum.mule.cluster.demo.service;

import com.fulcrum.mule.cluster.demo.model.Subscription;

public class SubscriptionService {
	public Subscription getSubscription() {
		Subscription subscription = new Subscription();
		subscription.setMinutes(100);
		subscription.setRate(20.00);
		
		return subscription;
	}
}
