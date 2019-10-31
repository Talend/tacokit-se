/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.azure.eventhubs.service;

import java.net.URI;
import java.util.ArrayList;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.components.azure.eventhubs.datastore.AzureEventHubsDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.IllegalEntityException;

@Service
public class UiActionService {

    @HealthCheck("checkEndpoint")
    public HealthCheckStatus checkEndpoint(@Option final AzureEventHubsDataStore conn, final Messages i18n) {
        EventHubClient ehClient = null;
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        try {
            final ConnectionStringBuilder connStr = new ConnectionStringBuilder()//
                    .setEndpoint(new URI(conn.getEndpoint()));//
            ehClient = EventHubClient.createSync(connStr.toString(), executorService);
            ehClient.closeSync();
        } catch (Throwable exception) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO,
                    i18n.healthCheckFailed("invalid endpoint or network issue!"));
        } finally {
            executorService.shutdown();
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.healthCheckOk());
    }

    @AsyncValidation("checkEventHub")
    public ValidationResult checkEventHub(@Option final AzureEventHubsDataStore connection, @Option final String eventHubName,
            final Messages i18n) {
        EventHubClient ehClient = null;
        ValidationResult result = new ValidationResult();
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        try {
            final ConnectionStringBuilder connStr = new ConnectionStringBuilder()//
                    .setEndpoint(new URI(connection.getEndpoint()))//
                    .setEventHubName(eventHubName)//
                    .setSasKeyName(connection.getSasKeyName())//
                    .setSasKey(connection.getSasKey());//
            ehClient = EventHubClient.createSync(connStr.toString(), executorService);
            ehClient.getRuntimeInformation().get();
        } catch (Throwable exception) {
            String koComment = "invalid endpoint or network issue!";
            Throwable caused = exception.getCause();
            if (caused != null && caused instanceof IllegalEntityException && caused.getMessage() != null) {
                koComment = caused.getMessage();
            } else {
                if (exception.getMessage() != null) {
                    koComment = exception.getMessage();
                }
            }
            result.setStatus(ValidationResult.Status.KO);
            result.setComment(koComment);
            return result;
        } finally {
            if (ehClient != null) {
                try {
                    ehClient.closeSync();
                } catch (EventHubException e) {
                    result.setStatus(ValidationResult.Status.KO);
                    result.setComment(e.getMessage());
                }
            }
            executorService.shutdown();
        }
        result.setStatus(ValidationResult.Status.OK);
        result.setComment("EventHub is available!");
        return result;
    }

    @Suggestions("listPartitionIds")
    public SuggestionValues listPartitionIds(@Option final AzureEventHubsDataSet dataset) {
        EventHubClient ehClient = null;
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        try {
            final ConnectionStringBuilder connStr = new ConnectionStringBuilder()//
                    .setEndpoint(new URI(dataset.getConnection().getEndpoint()))//
                    .setEventHubName(dataset.getEventHubName())//
                    .setSasKeyName(dataset.getConnection().getSasKeyName())//
                    .setSasKey(dataset.getConnection().getSasKey());//
            ehClient = EventHubClient.createSync(connStr.toString(), executorService);
            EventHubRuntimeInformation ehInfo = ehClient.getRuntimeInformation().get();
            List<SuggestionValues.Item> items = new ArrayList<>();
            String[] partitionIds = ehInfo.getPartitionIds();
            for (String partitionId : partitionIds) {
                items.add(new SuggestionValues.Item(partitionId, partitionId));
            }
            return new SuggestionValues(true, items);
        } catch (Throwable exception) {
            throw new IllegalStateException(exception);
        } finally {
            if (ehClient != null) {
                try {
                    ehClient.closeSync();
                } catch (EventHubException e) {
                    throw new IllegalStateException(e);
                }
            }
            executorService.shutdown();
        }
    }

    // This INCOMING_PATHS_DYNAMIC service is a flag for inject incoming paths dynamic, won't be called
    @DynamicValues("INCOMING_PATHS_DYNAMIC")
    public Values actions() {
        return new Values(Collections.EMPTY_LIST);
    }

}