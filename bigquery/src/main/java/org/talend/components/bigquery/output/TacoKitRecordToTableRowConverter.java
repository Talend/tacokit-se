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
package org.talend.components.bigquery.output;

import com.google.api.client.util.Base64;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.bigquery.service.BigQueryConnectorException;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Slf4j
public class TacoKitRecordToTableRowConverter {

    private com.google.cloud.bigquery.Schema tableSchema;

    private I18nMessage i18n;

    public TacoKitRecordToTableRowConverter(com.google.cloud.bigquery.Schema tableSchema, I18nMessage i18n) {
        this.tableSchema = tableSchema;
        this.i18n = i18n;
    }

    public Map<String, ?> apply(Record input) {
        if (input == null) {
            return null;
        }
        return convertRecordToTableRow(input);
    }

    private Map<String, ?> convertRecordToTableRow(Record input) {
        return convertRecordToTableRow(input, tableSchema.getFields());
    }

    private Map<String, ?> convertRecordToTableRow(Record input, FieldList fieldList) {
        if (input == null) {
            return null;
        }
        Map<String, Object> tableRow = new HashMap<>();
        final Schema schema = input.getSchema();
        for (Schema.Entry entry : schema.getEntries()) {
            String fieldName = entry.getName();
            Field field = fieldList.get(fieldName);
            if (input.get(Object.class, fieldName) == null) {
                tableRow.put(fieldName, null);
            } else {
                switch (entry.getType()) {
                case RECORD:
                    Optional.ofNullable(input.getRecord(fieldName))
                            .ifPresent(record -> tableRow.put(fieldName, convertRecordToTableRow(record, field.getSubFields())));
                    break;
                case ARRAY:
                    switch (entry.getElementSchema().getType()) {
                    case RECORD:
                        Collection<Record> records = input.getArray(Record.class, fieldName);
                        tableRow.put(fieldName, records.stream()
                                .map(record -> convertRecordToTableRow(record, field.getSubFields())).collect(toList()));
                        break;
                    case STRING:
                        tableRow.put(fieldName, input.getArray(String.class, fieldName));
                        break;
                    case BYTES:
                        tableRow.put(fieldName, input.getArray(byte[].class, fieldName).stream().map(Base64::encodeBase64String)
                                .collect(Collectors.toList()));
                        break;
                    case INT:
                        tableRow.put(fieldName, input.getArray(Integer.class, fieldName));
                        break;
                    case LONG:
                        tableRow.put(fieldName, input.getArray(Long.class, fieldName));
                        break;
                    case FLOAT:
                        tableRow.put(fieldName, input.getArray(Float.class, fieldName));
                        break;
                    case DOUBLE:
                        tableRow.put(fieldName, input.getArray(Double.class, fieldName));
                        break;
                    case BOOLEAN:
                        tableRow.put(fieldName, input.getArray(Boolean.class, fieldName));
                        break;
                    case DATETIME:
                        tableRow.put(fieldName,
                                input.getArray(ZonedDateTime.class, fieldName).stream()
                                        .map(dt -> Optional.ofNullable(dt).map(getDateFunction(field.getType())).orElse(null))
                                        .collect(toList()));
                        break;
                    default:
                        throw new BigQueryConnectorException(i18n.entryTypeNotDefined(entry.getElementSchema().getType().name()));
                    }
                    break;
                case STRING:
                    tableRow.put(fieldName, input.getString(fieldName));
                    break;
                case BYTES:
                    tableRow.put(fieldName, Base64.encodeBase64String(input.getBytes(fieldName)));
                    break;
                case INT:
                    tableRow.put(fieldName, input.getInt(fieldName));
                    break;
                case LONG:
                    tableRow.put(fieldName, input.getLong(fieldName));
                    break;
                case FLOAT:
                    tableRow.put(fieldName, input.getFloat(fieldName));
                    break;
                case DOUBLE:
                    tableRow.put(fieldName, input.getDouble(fieldName));
                    break;
                case BOOLEAN:
                    tableRow.put(fieldName, input.getBoolean(fieldName));
                    break;
                case DATETIME:
                    tableRow.put(fieldName, Optional.ofNullable(input.getDateTime(fieldName))
                            .map(dt -> getDateFunction(field.getType()).apply(dt)).orElse(null));
                    break;
                default:
                    throw new BigQueryConnectorException(i18n.entryTypeNotDefined(entry.getType().name()));
                }
            }
        }
        return tableRow;
    }

    private Function<ZonedDateTime, String> getDateFunction(LegacySQLTypeName fieldType) {
        Function<ZonedDateTime, String> func;
        if (LegacySQLTypeName.TIME.equals(fieldType)) {
            func = this::getTimeString;
        } else if (LegacySQLTypeName.DATETIME.equals(fieldType)) {
            func = this::getDateTimeString;
        } else if (LegacySQLTypeName.DATE.equals(fieldType)) {
            func = this::getDateString;
        } else {
            func = this::getTimestampString;
        }
        return func;
    }

    private String getTimeString(ZonedDateTime time) {
        return time.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    private String getDateTimeString(ZonedDateTime time) {
        return time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
    }

    private String getDateString(ZonedDateTime time) {
        return time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    private String getTimestampString(ZonedDateTime time) {
        return time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
    }

}
