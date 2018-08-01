// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.input;

import static org.slf4j.LoggerFactory.getLogger;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.ListClient;
import org.talend.sdk.component.api.configuration.Option;

public class ListSource extends MarketoSource {

    private final ListClient listClient;

    private transient static final Logger LOG = getLogger(ListClient.class);

    public ListSource(@Option("configuration") final MarketoInputDataSet dataSet, //
            final I18nMessage i18n, //
            final JsonBuilderFactory jsonFactory, //
            final JsonReaderFactory jsonReader, //
            final JsonWriterFactory jsonWriter, //
            final AuthorizationClient authorizationClient, //
            final ListClient listClient) {
        super(dataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient);
        this.listClient = listClient;
        this.listClient.base(this.dataSet.getDataStore().getEndpoint());
    }

    @Override
    public JsonObject runAction() {
        switch (dataSet.getListAction()) {
        case list:
            return getLists();
        case get:
            return getListById();
        case isMemberOf:
            return isMemberOfList();
        case getLeads:
            return getLeadsByListId();
        }

        throw new RuntimeException(i18n.invalidOperation());
    }

    private JsonObject getLeadsByListId() {
        Integer listId = dataSet.getListId();
        String fields = dataSet.getFields();
        Integer batchSize = dataSet.getBatchSize();
        return handleResponse(listClient.getLeadsByListId(accessToken, nextPageToken, listId, fields, batchSize));
    }

    private JsonObject isMemberOfList() {
        Integer listId = dataSet.getListId();
        String leadIds = dataSet.getLeadIds();
        return handleResponse(listClient.isMemberOfList(accessToken, listId, leadIds));
    }

    private JsonObject getListById() {
        Integer listId = dataSet.getListId();
        return handleResponse(listClient.getListbyId(accessToken, listId));
    }

    private JsonObject getLists() {
        Integer id = dataSet.getListId();
        String name = dataSet.getListName();
        String workspaceName = dataSet.getWorkspaceName();
        String programName = dataSet.getProgramName();
        Integer batchSize = dataSet.getBatchSize();
        return handleResponse(listClient.getLists(accessToken, nextPageToken, id, name, programName, workspaceName, batchSize));
    }
}
