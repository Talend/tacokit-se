package org.talend.components.jms.service;

import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.Values;

import static java.util.stream.Collectors.toList;

@Service
public class ActionService {

    public static final String ACTION_LIST_SUPPORTED_BROKER = "ACTION_LIST_SUPPORTED_BROKER";

    @Service
    private JmsService jmsService;

    @DynamicValues(ACTION_LIST_SUPPORTED_BROKER)
    public Values loadSupportedJMSProviders() {
        return new Values(jmsService.getProviders().keySet().stream().map(id -> new Values.Item(id, id)).collect(toList()));
    }
}
