package org.talend.components.zendesk.sources.delete;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.common.SelectionType;
import org.talend.components.zendesk.helpers.JsonHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.sources.Reject;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.zendesk.client.v2.model.Ticket;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "zendesk_delete")
@Processor(name = "Delete")
@Documentation("Data delete processor")
public class ZendeskDeleteProcessor implements Serializable {

    private final ZendeskDeleteConfiguration configuration;

    private ZendeskHttpClientService zendeskHttpClientService;

    private List<JsonObject> batchData = new ArrayList<>();

    private Messages i18n;

    public ZendeskDeleteProcessor(@Option("configuration") final ZendeskDeleteConfiguration configuration,
            final ZendeskHttpClientService zendeskHttpClientService, final Messages i18n) {
        this.configuration = configuration;
        this.zendeskHttpClientService = zendeskHttpClientService;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        if (configuration.getDataSet().getSelectionType() != SelectionType.TICKETS) {
            throw new UnsupportedOperationException(i18n.deleteUnsupportedType());
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        batchData.clear();
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        switch (configuration.getDataSet().getSelectionType()) {
        case TICKETS:
            batchData.add(record);
            break;
        }
    }

    @AfterGroup
    public void afterGroup(final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        if (!batchData.isEmpty()) {
            switch (configuration.getDataSet().getSelectionType()) {
            case TICKETS:
                zendeskHttpClientService.deleteTickets(configuration, batchData.stream()
                        .map(jsonObject -> JsonHelper.toInstance(jsonObject, Ticket.class)).collect(Collectors.toList()), success,
                        reject);
                break;
            }
        }
    }
}