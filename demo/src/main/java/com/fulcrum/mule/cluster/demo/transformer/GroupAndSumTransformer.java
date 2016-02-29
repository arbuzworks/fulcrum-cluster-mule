package com.fulcrum.mule.cluster.demo.transformer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fulcrum.mule.cluster.demo.model.Invoice;
import com.fulcrum.mule.cluster.demo.service.FeeService;
import com.fulcrum.mule.cluster.demo.service.SubscriptionService;
import com.fulcrum.mule.cluster.demo.service.TaxService;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

public class GroupAndSumTransformer extends AbstractMessageTransformer {

    public SubscriptionService subscriptionService = new SubscriptionService();
    public FeeService feeService = new FeeService();
    public TaxService taxService = new TaxService();

    @SuppressWarnings("unchecked")
    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {
        CopyOnWriteArrayList<String> payload = (CopyOnWriteArrayList<String>) message.getPayload();
        Map<String, Invoice> result = new HashMap<String, Invoice>();

        for (String record : payload) {
            String[] columns = record.split("[,]");
            if (result.containsKey(columns[0])) {
                result.get(columns[0]).addMinutes(Integer.parseInt(columns[1]));
            } else {
                result.put(columns[0], new Invoice(Integer.parseInt(columns[1])));
            }
        }

        for (Invoice invoice : result.values()) {
            invoice.setSubscription(subscriptionService.getSubscription());
            invoice.setFee(feeService.getFee());
            invoice.setTax(taxService.getTax());
            invoice.calculate();
        }
        return result;
    }
}