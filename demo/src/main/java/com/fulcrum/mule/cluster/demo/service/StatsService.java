package com.fulcrum.mule.cluster.demo.service;

import java.util.Collections;

import org.mule.api.MuleEventContext;
import org.mule.api.construct.FlowConstruct;
import org.mule.management.stats.FlowConstructStatistics;
import org.mule.management.stats.printers.XMLPrinter;
import org.mule.api.lifecycle.Callable;

public class StatsService implements Callable {

	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
	 	System.out.println("---> SPLITTER COMPLETED");
		return eventContext.getMessage().getPayload();
	}
	
}
