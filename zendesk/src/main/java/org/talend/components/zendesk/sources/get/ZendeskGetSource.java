package org.talend.components.zendesk.sources.get;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.helpers.CommonHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;
import java.io.Serializable;

@Slf4j
@Documentation("Input data processor")
public class ZendeskGetSource implements Serializable {

    private final ZendeskGetConfiguration configuration;

    private ZendeskHttpClientService zendeskHttpClientService;

    private InputIterator itemIterator;

    private Messages i18n;

    public ZendeskGetSource(@Option("configuration") final ZendeskGetConfiguration configuration,
            final ZendeskHttpClientService zendeskHttpClientService, final Messages i18n) {
        this.configuration = configuration;
        this.zendeskHttpClientService = zendeskHttpClientService;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        itemIterator = CommonHelper.getInputIterator(zendeskHttpClientService, configuration, i18n);
    }

    @Producer
    public JsonObject next() {
        return itemIterator.next();
    }
}