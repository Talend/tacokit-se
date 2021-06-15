/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.zendesk.output;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.json.JsonObject;

import org.talend.components.zendesk.common.SelectionType;
import org.talend.components.zendesk.helpers.JsonHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.source.Reject;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.zendesk.client.v2.model.Request;
import org.zendesk.client.v2.model.Ticket;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = CUSTOM, custom = "ZendeskOutput") // icon is located at src/main/resources/icons/ZendeskOutput.svg
@Processor(name = "ZendeskOutput")
@Documentation("Data put processor.")
public class ZendeskOutput implements Serializable {

    private final ZendeskOutputConfiguration configuration;

    private ZendeskHttpClientService zendeskHttpClientService;

    private List<Record> batchData = new ArrayList<>();

    private Messages i18n;

    private boolean useBatch;

    public ZendeskOutput(@Option("configuration") final ZendeskOutputConfiguration configuration,
            final ZendeskHttpClientService zendeskHttpClientService, final Messages i18n) {
        this.configuration = configuration;
        this.zendeskHttpClientService = zendeskHttpClientService;
        this.i18n = i18n;
        useBatch = configuration.isUseBatch() && configuration.getDataset().getSelectionType() == SelectionType.TICKETS;
    }

    @BeforeGroup
    public void beforeGroup() {
        batchData.clear();
    }

    @ElementListener
    public void onNext(@Input final Record record) throws IOException {
        if (useBatch || DataAction.DELETE.equals(configuration.getDataAction())) {
            batchData.add(record);
        } else {
            processOutputElement(record);
        }
    }

    private void processOutputElement(final Record record) {
        try {
            JsonObject newRecord;
            switch (configuration.getDataset().getSelectionType()) {
            case REQUESTS:
                Request item = JsonHelper.toInstance(record, Request.class);
                newRecord = zendeskHttpClientService.putRequest(configuration.getDataset().getDataStore(), item);
                break;
            case TICKETS:
                Ticket ticket = JsonHelper.toInstance(record, Ticket.class);
                newRecord = zendeskHttpClientService.putTicket(configuration.getDataset().getDataStore(), ticket);
                break;
            default:
                throw new ComponentException(i18n.UnknownTypeException());
            }
            checkNullResult(newRecord);
        } catch (Exception e) {
            log.error(e.getMessage());
            log.debug(Arrays.toString(e.getStackTrace()));
            log.error("Record rejected: {}", record.toString());
        }
    }

    private void checkNullResult(JsonObject newRecord) {
        if (newRecord == null) {
            throw new ComponentException(i18n.ObjectProcessError());
        }
    }

    @AfterGroup
    public void afterGroup() {
        if (!batchData.isEmpty() && SelectionType.TICKETS.equals(configuration.getDataset().getSelectionType())) {
            if (DataAction.CREATE.equals(configuration.getDataAction())) {
                zendeskHttpClientService.putTickets(configuration.getDataset().getDataStore(), batchData.stream()
                        .map(jsonObject -> JsonHelper.toInstance(jsonObject, Ticket.class)).collect(Collectors.toList()));
            } else {
                zendeskHttpClientService.deleteTickets(configuration, batchData.stream()
                        .map(jsonObject -> JsonHelper.toInstance(jsonObject, Ticket.class)).collect(Collectors.toList()));
            }

        }
    }

}