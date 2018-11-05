package org.talend.components.zendesk.sources.put;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.common.SelectionType;
import org.talend.components.zendesk.helpers.CommonHelper;
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
import org.zendesk.client.v2.model.Request;
import org.zendesk.client.v2.model.Ticket;

import javax.json.JsonObject;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "zendesk_put")
@Processor(name = "Output")
@Documentation("Data put processor")
public class ZendeskPutProcessor implements Serializable {

    private final ZendeskPutConfiguration configuration;

    private ZendeskHttpClientService zendeskHttpClientService;

    private List<JsonObject> batchData = new ArrayList<>();

    private Messages i18n;

    private boolean useBatch;

    public ZendeskPutProcessor(@Option("configuration") final ZendeskPutConfiguration configuration,
            final ZendeskHttpClientService zendeskHttpClientService, final Messages i18n) {
        this.configuration = configuration;
        this.zendeskHttpClientService = zendeskHttpClientService;
        this.i18n = i18n;
        useBatch = configuration.isUseBatch() && configuration.getDataSet().getSelectionType() == SelectionType.TICKETS;
    }

    @BeforeGroup
    public void beforeGroup() {
        batchData.clear();
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) throws IOException {
        if (useBatch) {
            batchData.add(record);
        } else {
            processOutputElement(record, success, reject);
        }
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject) {
        log.debug("processOutputElement_local: ");
        try {
            JsonObject newRecord;
            switch (configuration.getDataSet().getSelectionType()) {
            case REQUESTS:
                Request item = JsonHelper.toInstance(record, Request.class);
                newRecord = zendeskHttpClientService.putRequest(configuration.getDataSet().getDataStore(), item);
                break;
            case TICKETS:
                Ticket ticket = JsonHelper.toInstance(record, Ticket.class);
                newRecord = zendeskHttpClientService.putTicket(configuration.getDataSet().getDataStore(), ticket);
                break;
            default:
                throw new RuntimeException(i18n.UnknownTypeException());
            }
            checkNullResult(newRecord);
            success.emit(newRecord);
        } catch (Exception e) {
            CommonHelper.processException(e, record, reject);
        }
    }

    private void checkNullResult(JsonObject newRecord) {
        if (newRecord == null) {
            throw new RuntimeException("Object processing error.");
        }
    }

    @AfterGroup
    public void afterGroup(final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        if (!batchData.isEmpty()) {
            switch (configuration.getDataSet().getSelectionType()) {
            case TICKETS:
                zendeskHttpClientService.putTickets(
                        configuration.getDataSet().getDataStore(), batchData.stream()
                                .map(jsonObject -> JsonHelper.toInstance(jsonObject, Ticket.class)).collect(Collectors.toList()),
                        success, reject);
                break;
            }
        }
    }
}