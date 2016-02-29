package com.fulcrum.mule.cluster.demo.service;

import com.fulcrum.mule.cluster.demo.model.Fee;

public class FeeService {
	
	public Fee getFee() {
		Fee fee = new Fee();
		fee.setAmount(0.10);
		
		return fee;
	}
}
