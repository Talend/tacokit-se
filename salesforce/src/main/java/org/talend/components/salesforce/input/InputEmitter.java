/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.input;

import static java.util.stream.Collectors.joining;
import static org.talend.components.salesforce.dataset.QueryDataSet.SourceType.MODULE_SELECTION;
import static org.talend.components.salesforce.dataset.QueryDataSet.SourceType.SOQL_QUERY;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.talend.components.salesforce.BulkResultSet;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.service.BulkQueryService;
import org.talend.components.salesforce.service.Messages;
import org.talend.components.salesforce.service.SalesforceService;
import org.talend.components.salesforce.soql.SoqlQuery;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "SalesforceInput")
@Emitter(name = "Input")
@Documentation("Salesforce query input ")
public class InputEmitter implements Serializable {

    private final SalesforceService service;

    private final QueryDataSet dataset;

    private final LocalConfiguration localConfiguration;

    private BulkQueryService bulkQueryService;

    private BulkResultSet bulkResultSet;

    private RecordBuilderFactory recordBuilderFactory;

    private Messages messages;

    public InputEmitter(@Option("configuration") final QueryDataSet queryDataSet, final SalesforceService service,
            LocalConfiguration configuration, final RecordBuilderFactory recordBuilderFactory,
            final Messages messages) {
        this.service = service;
        this.dataset = queryDataSet;
        this.localConfiguration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.messages = messages;
    }

    @PostConstruct
    public void init() {
        try {
            final BulkConnection bulkConnection = service.bulkConnect(dataset.getDataStore(), localConfiguration);
            String moduleName = dataset.getModuleName();
            if (SOQL_QUERY.equals(dataset.getSourceType())) {
                moduleName = SalesforceService.guessModuleName(dataset.getQuery());
            }
            Map<String, Field> fieldMap = service.getFieldMap(dataset.getDataStore(), moduleName, localConfiguration);
            bulkQueryService = new BulkQueryService(bulkConnection, recordBuilderFactory, messages);
            bulkQueryService.setFieldMap(fieldMap);
            bulkQueryService.doBulkQuery(getModuleName(), getSoqlQuery());
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        } catch (AsyncApiException e) {
            throw new IllegalStateException(e.getExceptionMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Producer
    public Record next() {
        try {
            if (bulkResultSet == null) {
                bulkResultSet = bulkQueryService.getQueryResultSet(bulkQueryService.nextResultId());
            }
            Map<String, String> currentRecord = bulkResultSet.next();
            if (currentRecord == null) {
                String resultId = bulkQueryService.nextResultId();
                if (resultId != null) {
                    bulkResultSet = bulkQueryService.getQueryResultSet(resultId);
                    currentRecord = bulkResultSet.next();
                }
            }
            return bulkQueryService.convertRecord(currentRecord);
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        } catch (AsyncApiException e) {
            throw new IllegalStateException(e.getExceptionMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String getModuleName() {
        if (dataset.getSourceType() == MODULE_SELECTION) {
            return dataset.getModuleName();
        }
        String query = dataset.getQuery();
        if (query != null && !query.isEmpty()) {
            SoqlQuery soqlInstance = SoqlQuery.getInstance();
            soqlInstance.init(query);
            return soqlInstance.getDrivingEntityName();
        }

        throw new IllegalStateException("Module name can't be retrieved");
    }

    private String getSoqlQuery() {
        if (dataset.getSourceType() == SOQL_QUERY) {
            return dataset.getQuery();
        }

        List<String> allModuleFields;
        DescribeSObjectResult describeSObjectResult;
        try {
            final PartnerConnection connection = service.connect(dataset.getDataStore(), localConfiguration);
            describeSObjectResult = connection.describeSObject(dataset.getModuleName());
            allModuleFields = getColumnNames(describeSObjectResult);
        } catch (ConnectionException e) {
            if (ApiFault.class.isInstance(e)) {
                ApiFault fault = ApiFault.class.cast(e);
                throw new IllegalStateException(fault.getExceptionMessage(), e);
            }
            throw new IllegalStateException(e);
        }

        List<String> queryFields;
        List<String> selectedColumns = dataset.getSelectColumnIds();
        if (selectedColumns == null || selectedColumns.isEmpty()) {
            queryFields = allModuleFields;
        } else if (!allModuleFields.containsAll(selectedColumns)) { // ensure requested fields exist
            throw new IllegalStateException("columns { "
                    + selectedColumns.stream().filter(c -> !allModuleFields.contains(c)).collect(joining(",")) + " } "
                    + "doesn't exist in module '" + dataset.getModuleName() + "'");
        } else {
            queryFields = selectedColumns;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        int count = 0;
        for (String se : queryFields) {
            if (count++ > 0) {
                sb.append(", ");
            }
            sb.append(se);
        }
        sb.append(" from ");
        sb.append(dataset.getModuleName());
        if (dataset.getCondition() != null && !dataset.getCondition().isEmpty()) {
            sb.append(" where ");
            sb.append(dataset.getCondition());
        }
        return sb.toString();
    }

    private List<String> getColumnNames(DescribeSObjectResult in) {
        List<String> fields = new ArrayList<>();
        for (Field field : in.getFields()) {
            // filter the invalid compound columns for salesforce bulk query api
            if (field.getType() == FieldType.address || // no address
                    field.getType() == FieldType.location || // no location
                    // no picklist that has a parent
                    (field.getType() == FieldType.picklist && field.getCompoundFieldName() != null
                            && !field.getCompoundFieldName().trim().isEmpty())) {
                continue;
            }
            fields.add(field.getName());
        }
        return fields;
    }

}
