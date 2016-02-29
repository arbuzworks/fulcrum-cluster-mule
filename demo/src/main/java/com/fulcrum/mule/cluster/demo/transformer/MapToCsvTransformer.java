package com.fulcrum.mule.cluster.demo.transformer;

import com.fulcrum.mule.cluster.demo.model.Invoice;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapToCsvTransformer extends AbstractMessageTransformer {

    @SuppressWarnings("unchecked")
    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {
        StringBuilder result = new StringBuilder();
        Map<String, Invoice> map = (Map<String, Invoice>) message.getPayload();
        for (Map.Entry<String, Invoice> entry : map.entrySet()) {
            Invoice invoice = entry.getValue();
            result.append(entry.getKey()).append(",").append(invoice.getMinutes()).append(",").append(invoice.getTotalAsString()).append("\n");
        }
        return result.toString();
    }

}
