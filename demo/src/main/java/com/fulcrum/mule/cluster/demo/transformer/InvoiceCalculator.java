package com.fulcrum.mule.cluster.demo.transformer;

import com.fulcrum.mule.cluster.demo.model.Invoice;
import com.fulcrum.mule.cluster.demo.service.FeeService;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

import java.util.Map;

/**
 * Created on Feb 18, 2015
 *
 * @author Andrey Maryshev
 */
public class InvoiceCalculator extends AbstractMessageTransformer {

    @SuppressWarnings("unchecked")
    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {
        Map<String, Invoice> map = (Map<String, Invoice>) message.getPayload();
        for (Map.Entry<String, Invoice> entry : map.entrySet()) {
            entry.getValue().calculate();
        }
        return map;
    }

}
