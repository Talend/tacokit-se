/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.mongodb.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import lombok.extern.slf4j.Slf4j;

/**
 * Transform record to document object.
 */
@Slf4j
public class RecordToDocument {

    public Document fromRecord(Record record) {
        return convertRecordToDocument(record);
    }

    private Document convertRecordToDocument(Record record) {
        final Document document = new Document();

        for (Entry entry : record.getSchema().getEntries()) {
            final String fieldName = entry.getName();
            Object val = record.get(Object.class, fieldName);
            log.debug("[convertRecordToJsonObject] entry: {}; type: {}; value: {}.", fieldName, entry.getType(), val);
            if (null == val) {
                document.put(fieldName, null);
            } else {
                this.addField(document, record, entry);
            }
        }
        return document;
    }

    private List toArray(Collection<Object> objects) {
        List array = new ArrayList();
        for (Object obj : objects) {
            if (obj instanceof Collection) {
                List subArray = toArray((Collection) obj);
                array.add(subArray);
            } else if (obj instanceof String) {
                array.add((String) obj);
            } else if (obj instanceof Record) {
                Document subObject = convertRecordToDocument((Record) obj);
                array.add(subObject);
            } else if (obj instanceof Integer) {
                array.add((Integer) obj);
            } else if (obj instanceof Long) {
                array.add((Long) obj);
            } else if (obj instanceof Double) {
                array.add((Double) obj);
            } else if (obj instanceof Boolean) {
                array.add((Boolean) obj);
            } else {
                array.add(obj);
            }
        }
        return array;
    }

    private void addField(Document document, Record record, Entry entry) {
        final String fieldName = entry.getName();
        switch (entry.getType()) {
            case RECORD:
                final Record subRecord = record.getRecord(fieldName);
                document.put(fieldName, convertRecordToDocument(subRecord));
                break;
            case ARRAY:
                final Collection<Object> list = record.getArray(Object.class, fieldName);
                final List array = toArray(list);
                document.put(fieldName, array);
                break;
            case STRING:
                document.put(fieldName, record.getString(fieldName));
                break;
            case BYTES:
                document.put(fieldName, new String(record.getBytes(fieldName)));
                break;
            case INT:
                document.put(fieldName, record.getInt(fieldName));
                break;
            case LONG:
                document.put(fieldName, record.getLong(fieldName));
                break;
            case FLOAT:
                document.put(fieldName, record.getFloat(fieldName));
                break;
            case DOUBLE:
                document.put(fieldName, record.getDouble(fieldName));
                break;
            case BOOLEAN:
                document.put(fieldName, record.getBoolean(fieldName));
                break;
            case DATETIME:
                document.put(fieldName, record.getDateTime(fieldName).toString());
                break;
        }
    }

}