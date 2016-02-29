package com.fulcrum.mule.cluster.demo.service;

import com.fulcrum.mule.cluster.demo.model.Tax;

public class TaxService {
	public Tax getTax() {
		Tax tax = new Tax();
		tax.setRate(0.05);
		
		return tax;
	}
	
}
