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
package org.talend.components.salesforce.service;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.sforce.soap.partner.IField;

import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.components.salesforce.configuration.OutputConfig.OutputAction;
import org.talend.components.salesforce.service.operation.ConnectionFacade;
import org.talend.components.salesforce.service.operation.Delete;
import org.talend.components.salesforce.service.operation.Insert;
import org.talend.components.salesforce.service.operation.RecordsOperation;
import org.talend.components.salesforce.service.operation.Result;
import org.talend.components.salesforce.service.operation.ThresholdOperation;
import org.talend.components.salesforce.service.operation.Update;
import org.talend.components.salesforce.service.operation.Upsert;
import org.talend.components.salesforce.service.operation.converters.SObjectConverter;
import org.talend.components.salesforce.service.operation.converters.SObjectConvertorForUpdate;
import org.talend.components.salesforce.service.operation.converters.SObjectRelationShip;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SalesforceOutputService implements Serializable {

    private static final String ID = "Id";

    private final ThresholdOperation operation;

    protected boolean exceptionForErrors;

    private String moduleName;

    private Map<String, IField> fieldMap;

    private Messages messages;

    public SalesforceOutputService(final OutputConfig outputConfig, final ConnectionFacade cnx, final Messages msg) {

        this.moduleName = outputConfig.getModuleDataSet().getModuleName();

        final int commitLevel;
        if (outputConfig.isBatchMode()) {
            commitLevel = outputConfig.getCommitLevel();
        } else {
            commitLevel = 1;
        }
        this.exceptionForErrors = outputConfig.isExceptionForErrors();

        final RecordsOperation recordsOperation = this.buildOperation(cnx, outputConfig);
        this.operation = buildThreshold(commitLevel, recordsOperation);
        this.messages = msg;
    }

    private RecordsOperation buildOperation(final ConnectionFacade cnx, final OutputConfig cfg) {
        final SObjectConverter converter = new SObjectConverter(() -> this.fieldMap, this.moduleName);
        if (cfg.getOutputAction() == OutputAction.UPDATE) {
            return new Update(cnx, converter);
        }
        if (cfg.getOutputAction() == OutputAction.INSERT) {
            return new Insert(cnx, converter);
        }
        if (cfg.getOutputAction() == OutputAction.DELETE) {
            return new Delete(cnx);
        }
        if (cfg.getOutputAction() == OutputAction.UPSERT) {
            final SObjectConvertorForUpdate updateConv = new SObjectConvertorForUpdate(() -> this.fieldMap,
                    getReferenceFieldsMap(), this.moduleName, cfg.getUpsertKeyColumn());
            return new Upsert(cnx, updateConv, cfg.getUpsertKeyColumn());
        }
        throw new ComponentException("Unknow operation " + cfg.getOutputAction());
    }

    private ThresholdOperation buildThreshold(final int commitLevel, final RecordsOperation operation) {
        return new ThresholdOperation(operation, commitLevel);
    }

    public void write(Record record) throws IOException {
        if (record == null) {
            return;
        }
        final List<Result> results = this.operation.execute(record);
        if (results != null) {
            this.handleResults(results);
        }
    }

    private void handleResults(final List<Result> results) throws IOException {
        final String errors = results.stream().filter((Result r) -> !r.isOK()).map(Result::getErrorsString)
                .filter(Objects::nonNull).collect(Collectors.joining(", "));
        if (errors != null && errors.length() > 0) {
            final String failedPipeline = this.messages.failedPipeline(this.operation.name(), errors);
            log.error(failedPipeline);
            if (exceptionForErrors) {
                throw new IOException(failedPipeline);
            }
        }
    }

    /**
     * Make sure all record submit before end
     */
    public void finish() throws IOException {
        final List<Result> results = this.operation.terminate();
        if (results != null) {
            this.handleResults(results);
        }
    }

    private Map<String, SObjectRelationShip> getReferenceFieldsMap() {
        // Object columns = sprops.upsertRelationTable.columnName.getValue();
        // Map<String, Map<String, String>> referenceFieldsMap = null;
        // if (columns != null && columns instanceof List) {
        // referenceFieldsMap = new HashMap<>();
        // List<String> lookupFieldModuleNames = sprops.upsertRelationTable.lookupFieldModuleName.getValue();
        // List<String> lookupFieldNames = sprops.upsertRelationTable.lookupFieldName.getValue();
        // List<String> lookupRelationshipFieldNames =
        // sprops.upsertRelationTable.lookupRelationshipFieldName.getValue();
        // List<String> externalIdFromLookupFields = sprops.upsertRelationTable.lookupFieldExternalIdName.getValue();
        // for (int index = 0; index < ((List) columns).size(); index++) {
        // Map<String, String> relationMap = new HashMap<>();
        // relationMap.put("lookupFieldModuleName", lookupFieldModuleNames.get(index));
        // if (sprops.upsertRelationTable.isUseLookupFieldName() && lookupFieldNames != null) {
        // relationMap.put("lookupFieldName", lookupFieldNames.get(index));
        // }
        // relationMap.put("lookupRelationshipFieldName", lookupRelationshipFieldNames.get(index));
        // relationMap.put("lookupFieldExternalIdName", externalIdFromLookupFields.get(index));
        // referenceFieldsMap.put(((List<String>) columns).get(index), relationMap);
        // }
        // }
        // return referenceFieldsMap;
        return new HashMap<>();
    }

    public void setFieldMap(Map<String, IField> fieldMap) {
        this.fieldMap = fieldMap;
    }

}