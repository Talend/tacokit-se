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
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;

public class CompanySource extends MarketoSource {

    private final CompanyClient companyClient;

    public CompanySource(@Option("configuration") MarketoInputDataSet dataSet, //
            final I18nMessage i18n, //
            final JsonBuilderFactory jsonFactory, //
            final JsonReaderFactory jsonReader, //
            final JsonWriterFactory jsonWriter, //
            final AuthorizationClient authorizationClient, //
            final CompanyClient companyClient) {
        super(dataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient);
        this.companyClient = companyClient;
        this.companyClient.base(this.dataSet.getDataStore().getEndpoint());
    }

    private transient static final Logger LOG = getLogger(CompanySource.class);

    @Override
    public JsonObject runAction() {
        switch (dataSet.getOtherAction()) {
        case describe:
            return describeCompany();
        case list:
        case get:
            return getCompanies();
        }

        throw new RuntimeException(i18n.invalidOperation());
    }

    private JsonObject describeCompany() {
        return handleResponse(companyClient.describeCompanies(accessToken));
    }

    private JsonObject getCompanies() {
        String filterType = dataSet.getFilterType();
        String filterValues = dataSet.getFilterValues();
        String fields = dataSet.getFields();
        Integer batchSize = dataSet.getBatchSize();
        return handleResponse(
                companyClient.getCompanies(accessToken, filterType, filterValues, fields, batchSize, nextPageToken));
    }

}
