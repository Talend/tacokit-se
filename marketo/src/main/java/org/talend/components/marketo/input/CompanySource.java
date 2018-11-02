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

import static java.util.stream.Collectors.joining;
import static org.slf4j.LoggerFactory.getLogger;

import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.Toolbox;
import org.talend.sdk.component.api.configuration.Option;

public class CompanySource extends MarketoSource {

    private final CompanyClient companyClient;

    public CompanySource(@Option("configuration") MarketoInputDataSet dataSet, //
            final MarketoService service, //
            final Toolbox tools) {
        super(dataSet, service, tools);
        this.companyClient = service.getCompanyClient();
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
        String fields = dataSet.getFields() == null ? null : dataSet.getFields().stream().collect(joining(","));
        Integer batchSize = dataSet.getBatchSize();
        return handleResponse(
                companyClient.getCompanies(accessToken, filterType, filterValues, fields, batchSize, nextPageToken));
    }

}
