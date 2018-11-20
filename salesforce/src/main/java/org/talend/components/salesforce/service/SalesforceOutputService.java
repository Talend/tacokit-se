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

package org.talend.components.salesforce.service;

import static org.talend.components.salesforce.dataset.WriteDataSet.OutputAction.UPDATE;
import static org.talend.components.salesforce.dataset.WriteDataSet.OutputAction.UPSERT;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.talend.components.salesforce.dataset.WriteDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.CalendarCodec;
import com.sforce.ws.bind.DateCodec;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.types.Time;
import com.sforce.ws.util.Base64;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SalesforceOutputService implements Serializable {

    protected final int commitLevel;

    protected final List<Record> deleteItems;

    protected final List<Record> insertItems;

    protected final List<Record> upsertItems;

    protected final List<Record> updateItems;

    private final List<Record> successfulWrites = new ArrayList<>();

    private final List<Record> rejectedWrites = new ArrayList<>();

    private final List<String> nullValueFields = new ArrayList<>();

    private final RecordBuilderFactory factory;

    private final Messages messages;

    protected boolean exceptionForErrors;

    private PartnerConnection connection;

    private WriteDataSet.OutputAction outputAction;

    private String moduleName;

    private String upsertKeyColumn;

    private int dataCount;

    private int successCount;

    private int rejectCount;

    private CalendarCodec calendarCodec = new CalendarCodec();

    private DateCodec dateCodec = new DateCodec();

    private Map<String, Field> fieldMap;

    private static final String ID = "Id";

    public SalesforceOutputService(WriteDataSet outputConfig, PartnerConnection connection,
            final RecordBuilderFactory factory, Messages messages) {
        this.connection = connection;
        this.outputAction = outputConfig.getOutputAction();
        this.moduleName = outputConfig.getModuleName();
        this.messages = messages;
        this.factory = factory;
        if (outputConfig.isBatchMode()) {
            commitLevel = outputConfig.getCommitLevel();
        } else {
            commitLevel = 1;
        }
        this.exceptionForErrors = outputConfig.isExceptionForErrors();

        int arraySize = commitLevel * 2;
        deleteItems = new ArrayList<>(arraySize);
        insertItems = new ArrayList<>(arraySize);
        updateItems = new ArrayList<>(arraySize);
        upsertItems = new ArrayList<>(arraySize);
        if (UPSERT.equals(outputConfig.getOutputAction())) {
            upsertKeyColumn = outputConfig.getUpsertKeyColumn();
        } else {
            upsertKeyColumn = "";
        }
    }

    public void setUpsertKeyColumn(String upsertKeyColumn) {
        this.upsertKeyColumn = upsertKeyColumn;
    }

    public void write(Record record) throws IOException {
        dataCount++;
        if (record == null) {
            return;
        }
        switch (outputAction) {
        case INSERT:
            insert(record);
            break;
        case UPDATE:
            update(record);
            break;
        case UPSERT:
            upsert(record);
            break;
        case DELETE:
            delete(record);
        }
    }

    private SObject createSObject(Record input) {
        SObject so = new SObject();
        so.setType(moduleName);
        nullValueFields.clear();
        for (Schema.Entry field : input.getSchema().getEntries()) {
            // For "Id" column, we should ignore it for "INSERT" action
            if (!("Id".equals(field.getName()) && WriteDataSet.OutputAction.INSERT.equals(outputAction))) {
                Object value = input.get(Object.class, field.getName());
                // TODO need check
                Field sfField = fieldMap.get(field.getName());
                if (sfField == null) {
                    continue;
                }
                if (value != null && !value.toString().isEmpty()) {
                    addSObjectField(so, sfField.getName(), sfField.getType(), value);
                } else {
                    if (UPDATE.equals(outputAction)) {
                        nullValueFields.add(field.getName());
                    }
                }
            }
        }
        // TODO ignoreNull
        if (false) {
            so.setFieldsToNull(nullValueFields.toArray(new String[0]));
        }
        return so;
    }

    private SObject createSObjectForUpsert(Record input) {
        SObject so = new SObject();
        so.setType(moduleName);
        Map<String, Map<String, String>> referenceFieldsMap = getReferenceFieldsMap();
        nullValueFields.clear();
        for (Schema.Entry field : input.getSchema().getEntries()) {
            Object value = input.get(Object.class, field.getName());
            Field sfField = fieldMap.get(field.getName());
            /*
             * if (sfField == null) {
             * continue;
             * }
             */
            if (value != null && !"".equals(value.toString())) {
                if (referenceFieldsMap != null && referenceFieldsMap.get(field.getName()) != null) {
                    Map<String, String> relationMap = referenceFieldsMap.get(field.getName());
                    String lookupRelationshipFieldName = relationMap.get("lookupRelationshipFieldName");
                    so.setField(lookupRelationshipFieldName, null);
                    so.getChild(lookupRelationshipFieldName).setField("type", relationMap.get("lookupFieldModuleName"));
                    // No need get the real type. Because of the External IDs should not be special type in
                    // addSObjectField()
                    addSObjectField(so.getChild(lookupRelationshipFieldName),
                            relationMap.get("lookupFieldExternalIdName"), sfField.getType(), value);
                } else {
                    // Skip column "Id" for upsert, when "Id" is not specified as "upsertKey.Column"
                    if (!"Id".equals(field.getName()) || field.getName().equals(upsertKeyColumn)) {
                        if (sfField != null) {
                            // The real type is need in addSObjectField()
                            addSObjectField(so, sfField.getName(), sfField.getType(), value);
                        } else {
                            // This is keep old behavior, when set a field which is not exist.
                            // It would throw a exception for this.
                            addSObjectField(so, field.getName(), FieldType.string, value);
                        }
                    }
                }
            } else {
                if (referenceFieldsMap != null && referenceFieldsMap.get(field.getName()) != null) {
                    Map<String, String> relationMap = referenceFieldsMap.get(field.getName());
                    String lookupFieldName = relationMap.get("lookupFieldName");
                    if (lookupFieldName != null && !lookupFieldName.trim().isEmpty()) {
                        nullValueFields.add(lookupFieldName);
                    }
                } else if (!("Id".equals(field.getName()) || field.getName().equals(upsertKeyColumn))) {
                    nullValueFields.add(field.getName());
                }
            }
        }
        // TODO ignoreNull
        if (false) {
            so.setFieldsToNull(nullValueFields.toArray(new String[0]));
        }
        return so;
    }

    private void addSObjectField(XmlObject xmlObject, String fieldName, FieldType fieldType, Object value) {
        Object valueToAdd = value;
        // Convert stuff here
        // For Nillable base64 type field, we retrieve it as UNION type:[bytes,null]
        // So need to unwrap it and get its real type
        if (FieldType.base64.equals(fieldType)) {
            if ((value instanceof String) || (value instanceof byte[])) {
                byte[] base64Data = null;
                if (value instanceof byte[]) {
                    base64Data = (byte[]) value;
                } else {
                    base64Data = ((String) value).getBytes();
                }
                if (Base64.isBase64(new String(base64Data))) {
                    valueToAdd = Base64.decode(base64Data);
                }
            }
        }
        if (fieldName != null && valueToAdd instanceof String) {
            switch (fieldType) {
            case _boolean:
                xmlObject.setField(fieldName, Boolean.valueOf((String) valueToAdd));
                break;
            case _double:
            case percent:
                xmlObject.setField(fieldName, Double.valueOf((String) valueToAdd));
                break;
            case _int:
                xmlObject.setField(fieldName, Integer.valueOf((String) valueToAdd));
                break;
            case currency:
                xmlObject.setField(fieldName, new BigDecimal((String) valueToAdd));
                break;
            case date:
                xmlObject.setField(fieldName, dateCodec.deserialize((String) valueToAdd));
                break;
            case datetime:
                xmlObject.setField(fieldName, calendarCodec.deserialize((String) valueToAdd));
                break;
            case time:
                xmlObject.setField(fieldName, new Time((String) valueToAdd));
                break;
            case base64:
            default:
                xmlObject.setField(fieldName, valueToAdd);
                break;
            }
        } else {
            if (valueToAdd instanceof Date) {
                xmlObject.setField(fieldName, SalesforceRuntimeHelper.convertDateToCalendar((Date) valueToAdd, true));
            } else {
                xmlObject.setField(fieldName, valueToAdd);
            }
        }
    }

    private SaveResult[] insert(Record input) throws IOException {
        insertItems.add(input);
        if (insertItems.size() >= commitLevel) {
            return doInsert();
        }
        return null;
    }

    private SaveResult[] doInsert() throws IOException {
        if (insertItems.size() > 0) {
            // Clean the feedback records at each batch write.
            cleanWrites();
            SObject[] accs = new SObject[insertItems.size()];
            for (int i = 0; i < insertItems.size(); i++) {
                accs[i] = createSObject(insertItems.get(i));
            }

            String[] changedItemKeys = new String[accs.length];
            SaveResult[] saveResults;
            try {
                saveResults = connection.create(accs);
                if (saveResults != null && saveResults.length != 0) {
                    int batch_idx = -1;
                    for (int i = 0; i < saveResults.length; i++) {
                        ++batch_idx;
                        if (saveResults[i].getSuccess()) {
                            successCount++;
                        } else {
                            rejectCount++;
                        }
                    }
                }
                insertItems.clear();
                return saveResults;
            } catch (ConnectionException e) {
                throw new IOException(e);
            }
        }
        return null;
    }

    private SaveResult[] update(Record input) throws IOException {
        updateItems.add(input);
        if (updateItems.size() >= commitLevel) {
            return doUpdate();
        }
        return null;
    }

    private SaveResult[] doUpdate() throws IOException {
        if (updateItems.size() > 0) {
            // Clean the feedback records at each batch write.
            cleanWrites();
            SObject[] upds = new SObject[updateItems.size()];
            for (int i = 0; i < updateItems.size(); i++) {
                upds[i] = createSObject(updateItems.get(i));
            }

            String[] changedItemKeys = new String[upds.length];
            for (int ix = 0; ix < upds.length; ++ix) {
                changedItemKeys[ix] = upds[ix].getId();
            }
            SaveResult[] saveResults;
            try {
                saveResults = connection.update(upds);
                upds = null;
                if (saveResults != null && saveResults.length != 0) {
                    int batch_idx = -1;
                    for (int i = 0; i < saveResults.length; i++) {
                        ++batch_idx;
                        if (saveResults[i].getSuccess()) {
                            successCount++;
                        } else {
                            rejectCount++;
                        }
                    }
                }
                updateItems.clear();
                return saveResults;
            } catch (ConnectionException e) {
                throw new IOException(e);
            }
        }
        return null;
    }

    private UpsertResult[] upsert(Record input) throws IOException {
        upsertItems.add(input);
        if (upsertItems.size() >= commitLevel) {
            return doUpsert();
        }
        return null;
    }

    private UpsertResult[] doUpsert() throws IOException {
        if (upsertItems.size() > 0) {
            // Clean the feedback records at each batch write.
            cleanWrites();
            SObject[] upds = new SObject[upsertItems.size()];
            for (int i = 0; i < upsertItems.size(); i++) {
                upds[i] = createSObjectForUpsert(upsertItems.get(i));
            }

            String[] changedItemKeys = new String[upds.length];
            for (int ix = 0; ix < upds.length; ++ix) {
                Object value = upds[ix].getField(upsertKeyColumn);
                if (value != null) {
                    changedItemKeys[ix] = String.valueOf(value);
                }
            }
            UpsertResult[] upsertResults;
            try {
                upsertResults = connection.upsert(upsertKeyColumn, upds);
                upds = null;
                if (upsertResults != null && upsertResults.length != 0) {
                    int batch_idx = -1;
                    for (int i = 0; i < upsertResults.length; i++) {
                        ++batch_idx;
                        if (upsertResults[i].getSuccess()) {
                            successCount++;
                        } else {
                            rejectCount++;
                        }
                    }
                }
                upsertItems.clear();
                return upsertResults;
            } catch (ConnectionException e) {
                throw new IOException(e);
            }
        }
        return null;

    }

    private DeleteResult[] delete(Record input) throws IOException {
        // Calculate the field position of the Id the first time that it is used. The Id field must be present in the
        // schema to delete rows.
        boolean containsId = false;
        for (Schema.Entry field : input.getSchema().getEntries()) {
            if (ID.equals(field.getName())) {
                containsId = true;
                break;
            }
        }
        if (containsId) {
            String id = input.getString(ID);
            if (id != null) {
                deleteItems.add(input);
                if (deleteItems.size() >= commitLevel) {
                    return doDelete();
                }
            }
            return null;
        } else {
            throw new RuntimeException("'Id' field not found!");
        }

    }

    private DeleteResult[] doDelete() throws IOException {
        if (deleteItems.size() > 0) {
            // Clean the feedback records at each batch write.
            cleanWrites();
            String[] delIDs = new String[deleteItems.size()];
            for (int ix = 0; ix < delIDs.length; ++ix) {
                delIDs[ix] = deleteItems.get(ix).getString(ID);
            }
            DeleteResult[] dr;
            try {
                dr = connection.delete(delIDs);
                if (dr != null && dr.length != 0) {
                    for (int i = 0; i < dr.length; i++) {
                        if (dr[i].getSuccess()) {
                            successCount++;
                        } else {
                            rejectCount++;
                        }
                    }
                }
                deleteItems.clear();
                return dr;
            } catch (ConnectionException e) {
                throw new IOException(e);
            }
        }
        return null;
    }

    private void logout() throws IOException {
        // Finish anything uncommitted
        doInsert();
        doDelete();
        doUpdate();
        doUpsert();
    }

    private Map<String, Map<String, String>> getReferenceFieldsMap() {
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

    public List<Record> getSuccessfulWrites() {
        return Collections.unmodifiableList(successfulWrites);
    }

    public List<Record> getRejectedWrites() {
        return Collections.unmodifiableList(rejectedWrites);
    }

    public void cleanWrites() {
        successfulWrites.clear();
        rejectedWrites.clear();
    }

    public void setFieldMap(Map<String, Field> fieldMap) {
        this.fieldMap = fieldMap;
    }

}