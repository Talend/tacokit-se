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

import static org.talend.components.salesforce.configuration.OutputConfig.OutputAction.UPDATE;
import static org.talend.components.salesforce.configuration.OutputConfig.OutputAction.UPSERT;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.CalendarCodec;
import com.sforce.ws.bind.DateCodec;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.types.Time;
import com.sforce.ws.util.Base64;

import org.apache.avro.util.Utf8;
import org.talend.components.salesforce.commons.SalesforceRuntimeHelper;
import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.components.salesforce.configuration.OutputConfig.OutputAction;
import org.talend.components.salesforce.service.operation.ConnectionFacade;
import org.talend.components.salesforce.service.operation.ConnectionFacade.ConnectionImpl;
import org.talend.components.salesforce.service.operation.Delete;
import org.talend.components.salesforce.service.operation.Insert;
import org.talend.components.salesforce.service.operation.RecordsOperation;
import org.talend.components.salesforce.service.operation.ThresholdOperation;
import org.talend.components.salesforce.service.operation.Update;
import org.talend.components.salesforce.service.operation.Upsert;
import org.talend.components.salesforce.service.operation.converters.SObjectConverter;
import org.talend.components.salesforce.service.operation.converters.SObjectConvertorForUpdate;
import org.talend.components.salesforce.service.operation.converters.SObjectRelationShip;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SalesforceOutputService implements Serializable {

    private static final String ID = "Id";

    protected final int commitLevel;

    private final Map<OutputConfig.OutputAction, ThresholdOperation> operations;

    private final List<Record> successfulWrites = new ArrayList<>();

    private final List<Record> rejectedWrites = new ArrayList<>();

    private final List<String> nullValueFields = new ArrayList<>();

    private final Messages messages;

    protected boolean exceptionForErrors;

    private PartnerConnection connection;

    private OutputConfig.OutputAction outputAction;

    private String moduleName;

    private boolean isBatchMode;

    private int dataCount;

    private int successCount;

    private int rejectCount;

    private CalendarCodec calendarCodec = new CalendarCodec();

    private DateCodec dateCodec = new DateCodec();

    private Map<String, Field> fieldMap;

    public SalesforceOutputService(OutputConfig outputConfig, PartnerConnection connection, Messages messages) {
        this.connection = connection;
        this.outputAction = outputConfig.getOutputAction();
        this.moduleName = outputConfig.getModuleDataSet().getModuleName();
        this.messages = messages;
        this.isBatchMode = outputConfig.isBatchMode();
        if (isBatchMode) {
            commitLevel = outputConfig.getCommitLevel();
        } else {
            commitLevel = 1;
        }
        this.exceptionForErrors = outputConfig.isExceptionForErrors();

        this.operations = new HashMap<>();
        SObjectConverter converter = new SObjectConverter(() -> this.fieldMap, this.moduleName);
        SObjectConvertorForUpdate updateConv = new SObjectConvertorForUpdate(() -> this.fieldMap,
                getReferenceFieldsMap(),
                this.moduleName,
                outputConfig.getUpsertKeyColumn());
        ConnectionFacade cnx = new ConnectionImpl(this.connection);
        this.operations.put(UPDATE, buildThreshold(new Update(cnx, converter)));
        this.operations.put(OutputAction.INSERT, buildThreshold(new Insert(cnx, converter)));
        this.operations.put(UPSERT, buildThreshold(new Upsert(cnx, updateConv, outputConfig.getUpsertKeyColumn())));
        this.operations.put(UPSERT, buildThreshold(new Delete(cnx)));
    }

    private ThresholdOperation buildThreshold(RecordsOperation operation) {
        return new ThresholdOperation(operation, this.commitLevel);
    }

    public void write(Record record) throws IOException {
        dataCount++;
        if (record == null) {
            return;
        }
        this.operations.get(this.outputAction).execute(record);
    }

    /**
     * Make sure all record submit before end
     */
    public void finish() throws IOException {
        this.operations.get(this.outputAction).terminate();
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

    public void cleanWrites() {
        successfulWrites.clear();
        rejectedWrites.clear();
    }

    public void setFieldMap(Map<String, Field> fieldMap) {
        this.fieldMap = fieldMap;
    }

}