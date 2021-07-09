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
package org.talend.components.zendesk.service.http;

import static java.lang.Thread.sleep;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;

import org.asynchttpclient.ListenableFuture;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.components.zendesk.helpers.CommonHelper;
import org.talend.components.zendesk.helpers.JsonHelper;
import org.talend.components.zendesk.output.PagedList;
import org.talend.components.zendesk.output.ZendeskOutputConfiguration;
import org.talend.components.zendesk.service.zendeskclient.ZendeskClientService;
import org.talend.components.zendesk.source.InputIterator;
import org.talend.components.zendesk.source.ZendeskInputMapperConfiguration;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.Service;
import org.zendesk.client.v2.Zendesk;
import org.zendesk.client.v2.model.JobStatus;
import org.zendesk.client.v2.model.Request;
import org.zendesk.client.v2.model.Ticket;
import org.zendesk.client.v2.model.User;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ZendeskHttpClientService {

    /**
     * max allowed size for 'create items' batch in Zendesk API (see documentation)
     */
    private final int MAX_ALLOWED_BATCH_SIZE = 100;

    @Service
    private ZendeskClientService zendeskClientService;

    @Service
    private JsonReaderFactory jsonReaderFactory;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    public User getCurrentUser(ZendeskDataStore dataStore) {
        log.debug("get current user");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(dataStore);
        User user = zendeskServiceClient.getCurrentUser();
        return user;
    }

    public InputIterator getRequests(ZendeskDataStore dataStore) {
        log.debug("get requests");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(dataStore);
        Iterable<Request> data = zendeskServiceClient.getRequests();
        return new InputIterator(data.iterator(), jsonReaderFactory);
    }

    public JsonObject putRequest(ZendeskDataStore dataStore, Request request) {
        log.debug("put requests");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(dataStore);
        Request newItem = zendeskServiceClient.createRequest(request);
        return JsonHelper.toJsonObject(newItem, jsonReaderFactory);
    }

    public InputIterator getTickets(ZendeskInputMapperConfiguration configuration) {
        log.debug("get tickets");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(configuration.getDataset().getDataStore());
        Iterable<Ticket> data;
        if (configuration.isQueryStringEmpty()) {
            data = zendeskServiceClient.getTickets();
        } else {
            // the result is available on Zendesk server after indexing
            data = zendeskServiceClient.getSearchResults(Ticket.class, configuration.getQueryString());
        }
        return new InputIterator(data.iterator(), jsonReaderFactory);
    }

    public JsonObject putTicket(ZendeskDataStore dataStore, Ticket ticket) {
        log.debug("put ticket");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(dataStore);
        Ticket newItem;
        if (ticket.getId() == null) {
            newItem = zendeskServiceClient.createTicket(ticket);
        } else {
            newItem = zendeskServiceClient.updateTicket(ticket);
        }
        return JsonHelper.toJsonObject(newItem, jsonReaderFactory);
    }

    public void putTickets(ZendeskDataStore dataStore, List<Ticket> tickets) {
        log.debug("put tickets");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(dataStore);

        PagedList<Ticket> pagedList = new PagedList<>(tickets, MAX_ALLOWED_BATCH_SIZE);
        List<Ticket> listPage;
        while ((listPage = pagedList.getNextPage()) != null) {
            putTicketsPage(zendeskServiceClient, listPage);
        }
    }

    private void putTicketsPage(Zendesk zendeskServiceClient, List<Ticket> tickets) {
        List<Ticket> ticketsUpdate = new ArrayList<>();
        List<Ticket> ticketsCreate = new ArrayList<>();
        tickets.forEach(ticket -> {
            if (ticket.getId() != null) {
                ticketsUpdate.add(ticket);
            } else {
                ticketsCreate.add(ticket);
            }
        });
        // removed JobStatus<Ticket>
        ListenableFuture<JobStatus> updateFuture = ticketsUpdate.isEmpty() ? null
                : zendeskServiceClient.updateTicketsAsync(ticketsUpdate);
        ListenableFuture<JobStatus> createFuture = ticketsCreate.isEmpty() ? null
                : zendeskServiceClient.createTicketsAsync(ticketsCreate);

        try {
            processJobStatusFuture(updateFuture, zendeskServiceClient, ticketsUpdate);
            processJobStatusFuture(createFuture, zendeskServiceClient, ticketsCreate);
        } catch (Exception e) {
            throw new ComponentException(e);
        }
    }

    public void deleteTickets(ZendeskOutputConfiguration configuration, List<Ticket> tickets) {
        log.debug("delete tickets");
        Zendesk zendeskServiceClient = zendeskClientService.getZendeskClientWrapper(configuration.getDataset().getDataStore());
        PagedList<Ticket> pagedList = new PagedList<>(tickets, MAX_ALLOWED_BATCH_SIZE);
        List<Ticket> listPage;
        while ((listPage = pagedList.getNextPage()) != null) {
            deleteTicketsPage(zendeskServiceClient, listPage);
        }
    }

    private void deleteTicketsPage(Zendesk zendeskServiceClient, List<Ticket> tickets) {
        if (tickets == null || tickets.isEmpty()) {
            return;
        }

        Optional<Long> firstId = Optional.ofNullable(tickets.get(0).getId());

        Long[] idArray = tickets.stream().map(Request::getId).toArray(Long[]::new);

        try {
            if (!firstId.isPresent()) {
                log.error("Field ID is missing for record {}", tickets.get(0));
            } else {
                zendeskServiceClient.deleteTickets(firstId.get(), CommonHelper.toPrimitives(idArray));
            }
        } catch (Exception e) {
            throw new ComponentException(e);
        }
    }

    private void processJobStatusFuture(ListenableFuture<JobStatus> future, Zendesk zendeskServiceClient, List<Ticket> tickets)
            throws ExecutionException, InterruptedException {
        if (future == null)
            return;
        JobStatus jobStatus = future.get();
        processJobStatus(jobStatus, zendeskServiceClient, tickets);
    }

    private void processJobStatus(JobStatus ticketJobStatus, Zendesk zendeskServiceClient, List<Ticket> tickets)
            throws InterruptedException {
        if (ticketJobStatus == null)
            return;

        JobStatus jobStatus = new JobStatus();
        jobStatus.setId(ticketJobStatus.getId());
        jobStatus.setUrl(ticketJobStatus.getUrl());
        jobStatus.setStatus(ticketJobStatus.getStatus());

        while (jobStatus.getStatus() == JobStatus.JobStatusEnum.queued
                || jobStatus.getStatus() == JobStatus.JobStatusEnum.working) {
            sleep(1000);
            jobStatus = zendeskServiceClient.getJobStatus(jobStatus);
        }
        if (jobStatus.getStatus() == JobStatus.JobStatusEnum.completed) {
            jobStatus.getResults().forEach(updateResult -> {
                log.info("updateResult was processed: " + updateResult.getId());
            });
        } else {
            throw new ComponentException("Batch processing failed. " + jobStatus.getMessage() + ". Failed item: "
                    + tickets.get(ticketJobStatus.getProgress()));
        }
    }

}
