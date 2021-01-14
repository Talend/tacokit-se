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
package org.talend.components.bigquery.service;

import com.google.api.client.util.Base64;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableField;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.output.BigQueryOutputConfig;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class BigQueryService {

    public static final String ACTION_SUGGESTION_DATASET = "BigQueryDataSet";

    public static final String ACTION_SUGGESTION_TABLES = "BigQueryTables";

    public static final String ACTION_HEALTH_CHECK = "HEALTH_CHECK";

    private static final String FIELDS = "\"fields\"";

    private static final String SUB_FIELDS = "\"subFields\"";

    @Service
    private RecordBuilderFactory recordBuilderFactoryService;

    @Service
    private I18nMessage i18n;

    public RecordBuilderFactory getRecordBuilderFactoryService() {
        return recordBuilderFactoryService;
    }

    @HealthCheck(ACTION_HEALTH_CHECK)
    public HealthCheckStatus healthCheck(@Option BigQueryConnection connection) {

        if (connection.getProjectName() == null || "".equals(connection.getProjectName().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.projectNameRequired());
        }
        if (connection.getJsonCredentials() == null || "".equals(connection.getJsonCredentials().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.credentialsRequired());
        }

        try {
            BigQuery client = createClient(connection);
            client.listDatasets(DatasetListOption.pageSize(1));
        } catch (final Exception e) {
            log.error("[HealthCheckStatus] {}", e.getMessage());
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

    @Suggestions(ACTION_SUGGESTION_DATASET)
    public SuggestionValues findDataSets(@Option("connection") final BigQueryConnection connection) {
        final BigQuery client = createClient(connection);
        try {
            return new SuggestionValues(true,
                    StreamSupport
                            .stream(client.listDatasets(connection.getProjectName(), DatasetListOption.pageSize(100)).getValues()
                                    .spliterator(), false)
                            .map(dataset -> new SuggestionValues.Item(dataset.getDatasetId().getDataset(),
                                    ofNullable(dataset.getFriendlyName()).orElseGet(() -> dataset.getDatasetId().getDataset())))
                            .collect(toList()));
        } catch (Exception e) {
            return new SuggestionValues(false, Collections.emptyList());
        }
    }

    @Suggestions(ACTION_SUGGESTION_TABLES)
    public SuggestionValues findTables(@Option("connection") final BigQueryConnection connection,
            @Option("bqDataset") final String bqDataset) {
        final BigQuery client = createClient(connection);
        try {
            return new SuggestionValues(true,
                    StreamSupport
                            .stream(client.listTables(DatasetId.of(connection.getProjectName(), bqDataset),
                                    BigQuery.TableListOption.pageSize(100)).getValues().spliterator(), false)
                            .map(table -> new SuggestionValues.Item(table.getTableId().getTable(),
                                    ofNullable(table.getFriendlyName()).orElseGet(() -> table.getTableId().getTable())))
                            .collect(toList()));
        } catch (Exception e) {
            return new SuggestionValues(false, Collections.emptyList());
        }
    }

    public BigQuery createClient(final BigQueryConnection connection) {

        BigQuery client = null;

        if (connection.getJsonCredentials() != null && !"".equals(connection.getJsonCredentials().trim())) {
            GoogleCredentials credentials = getCredentials(connection.getJsonCredentials());

            client = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(connection.getProjectName()).build()
                    .getService();
        } else {
            client = BigQueryOptions.getDefaultInstance().getService();
        }

        return client;
    }

    public com.google.cloud.bigquery.Schema guessSchema(BigQueryOutputConfig configuration) {
        BigQuery client = createClient(configuration.getDataSet().getConnection());
        Table table = client.getTable(configuration.getDataSet().getBqDataset(), configuration.getDataSet().getTableName(),
                TableOption.fields(TableField.SCHEMA));
        if (table != null) {
            return table.getDefinition().getSchema();
        } else {
            log.warn(i18n.schemaNotDefined());
            return null;
        }
    }

    public com.google.cloud.bigquery.Schema guessSchema(Record record) {

        if (record.getSchema() != null) {
            return convertToGoogleSchema(record.getSchema());
        }

        return null;
    }

    public org.talend.sdk.component.api.record.Schema convertToTckSchema(Schema gSchema) {
        org.talend.sdk.component.api.record.Schema.Builder schemaBuilder = recordBuilderFactoryService
                .newSchemaBuilder(org.talend.sdk.component.api.record.Schema.Type.RECORD);

        gSchema.getFields().stream()
                .forEach(f -> schemaBuilder.withEntry(recordBuilderFactoryService.newEntryBuilder().withName(f.getName())
                        .withType(convertToTckType(f.getType(), f.getMode())).withElementSchema(getSubSchema(f))
                        .withNullable(true).build()));

        return schemaBuilder.build();
    }

    public org.talend.sdk.component.api.record.Schema getSubSchema(Field f) {

        if (!f.getType().equals(LegacySQLTypeName.RECORD)) {
            return recordBuilderFactoryService.newSchemaBuilder(convertToTckType(f.getType(), Field.Mode.NULLABLE)).build();
        }

        org.talend.sdk.component.api.record.Schema.Builder schemaBuilder = recordBuilderFactoryService
                .newSchemaBuilder(org.talend.sdk.component.api.record.Schema.Type.RECORD);

        if (f.getSubFields() != null && !f.getSubFields().isEmpty()) {
            f.getSubFields().stream()
                    .forEach(inner -> schemaBuilder.withEntry(recordBuilderFactoryService.newEntryBuilder()
                            .withName(inner.getName()).withType(convertToTckType(inner.getType(), inner.getMode()))
                            .withElementSchema(getSubSchema(inner)).withNullable(true).build()));
        }

        return schemaBuilder.build();
    }

    public Schema convertToGoogleSchema(org.talend.sdk.component.api.record.Schema tckSchema) {
        return Schema.of(tckSchema.getEntries().stream()
                .map(e -> Field.newBuilder(e.getName(), convertToGoogleType(e.getType(), e.getElementSchema()), getSubFields(e))
                        .setMode(e.getType() == org.talend.sdk.component.api.record.Schema.Type.ARRAY ? Field.Mode.REPEATED
                                : Field.Mode.NULLABLE)
                        .build())
                .collect(Collectors.toList()));
    }

    public Field[] getSubFields(org.talend.sdk.component.api.record.Schema.Entry entry) {
        if (entry.getType() == org.talend.sdk.component.api.record.Schema.Type.RECORD
                || entry.getType() == org.talend.sdk.component.api.record.Schema.Type.ARRAY) {
            List<Field> subFields = new ArrayList<>(convertToGoogleSchema(entry.getElementSchema()).getFields());
            return subFields.toArray(new Field[subFields.size()]);

        }

        return new Field[] {};

    }

    public LegacySQLTypeName convertToGoogleType(org.talend.sdk.component.api.record.Schema.Type type,
            org.talend.sdk.component.api.record.Schema subSchema) {
        switch (type) {
        case ARRAY:
            return convertToGoogleType(subSchema.getType(), subSchema.getElementSchema());
        case RECORD:
            return LegacySQLTypeName.RECORD;
        case BOOLEAN:
            return LegacySQLTypeName.BOOLEAN;
        case BYTES:
            return LegacySQLTypeName.BYTES;
        case DATETIME:
            return LegacySQLTypeName.DATETIME;
        case DOUBLE:
            return LegacySQLTypeName.FLOAT;
        case LONG:
            return LegacySQLTypeName.INTEGER;
        default:
            return LegacySQLTypeName.STRING;
        }
    }

    public org.talend.sdk.component.api.record.Schema.Type convertToTckType(LegacySQLTypeName type, Field.Mode mode) {

        if (mode == Field.Mode.REPEATED) {
            return org.talend.sdk.component.api.record.Schema.Type.ARRAY;
        }

        switch (type.name()) {
        case "RECORD":
            return org.talend.sdk.component.api.record.Schema.Type.RECORD;
        case "BOOLEAN":
            return org.talend.sdk.component.api.record.Schema.Type.BOOLEAN;
        case "BYTES":
            return org.talend.sdk.component.api.record.Schema.Type.BYTES;
        case "DATETIME":
        case "DATE":
        case "TIMESTAMP":
        case "TIME":
            return org.talend.sdk.component.api.record.Schema.Type.DATETIME;
        case "FLOAT":
            return org.talend.sdk.component.api.record.Schema.Type.DOUBLE;
        case "INTEGER":
            return org.talend.sdk.component.api.record.Schema.Type.LONG;
        default:
            return org.talend.sdk.component.api.record.Schema.Type.STRING;
        }
    }

    public void convertToTckField(FieldValueList fieldValueList, Record.Builder rb, Field f, Schema tableSchema) {
        String name = f.getName();
        FieldValue value = fieldValueList.get(name);

        if (value != null && !value.isNull()) {
            LegacySQLTypeName type = f.getType();

            if (f.getMode() == Field.Mode.REPEATED) {
                // ARRAY
                Schema subSchema = tableSchema.getFields().stream().filter(field -> field.getName().equals(name)).map(
                        field -> Schema.of(field.getSubFields() != null ? field.getSubFields() : Collections.singleton(field)))
                        .findFirst().get();
                org.talend.sdk.component.api.record.Schema.Entry entry = recordBuilderFactoryService.newEntryBuilder()
                        .withName(name).withType(org.talend.sdk.component.api.record.Schema.Type.ARRAY).withNullable(true)
                        .withElementSchema(convertToTckSchema(subSchema)).build();

                switch (type.name()) {

                case "RECORD":

                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> {
                        FieldValueList ifv = fv.getRecordValue();
                        FieldList ifs = f.getSubFields();

                        FieldValueList innerFieldsValueWithSchema = FieldValueList.of(
                                StreamSupport.stream(ifv.spliterator(), false).collect(Collectors.toList()),
                                subSchema.getFields());
                        Record.Builder innerRecordBuilder = recordBuilderFactoryService
                                .newRecordBuilder(convertToTckSchema(subSchema));
                        ifs.stream().forEach(innerField -> convertToTckField(innerFieldsValueWithSchema, innerRecordBuilder,
                                innerField, subSchema));
                        return innerRecordBuilder.build();

                    }).collect(Collectors.toList()));
                    break;
                case "BOOLEAN":
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> fv.getBooleanValue())
                            .collect(Collectors.toList()));
                    break;
                case "BYTES":
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream()
                            .map(fv -> Base64.decodeBase64(fv.getStringValue())).collect(Collectors.toList()));
                    break;
                case "TIMESTAMP":
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> fv.getTimestampValue() / 1000)
                            .collect(Collectors.toList()));
                    break;
                case "DATE":
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> {
                        try {
                            return sdf.parse(fv.getStringValue());
                        } catch (ParseException e) {
                            log.warn("Cannot parse date {}", fv.getStringValue());
                            return null;
                        }
                    }).collect(Collectors.toList()));
                    break;
                case "DATETIME":
                    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> {
                        try {
                            return sdf.parse(fv.getStringValue());
                        } catch (ParseException e) {
                            log.warn("Cannot parse datetime {}", fv.getStringValue());
                            return null;
                        }
                    }).collect(Collectors.toList()));
                    break;
                case "FLOAT":
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> fv.getDoubleValue())
                            .collect(Collectors.toList()));
                    break;
                case "INTEGER":
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> fv.getLongValue())
                            .collect(Collectors.toList()));
                    break;
                case "TIME":
                    sdf = new SimpleDateFormat("HH:mm:ss");
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> {
                        try {
                            return sdf.parse(fv.getStringValue());
                        } catch (ParseException e) {
                            log.warn("Cannot parse time {}", fv.getStringValue());
                            return null;
                        }
                    }).collect(Collectors.toList()));
                    break;
                default:
                    rb.withArray(entry, ((FieldValueList) value.getValue()).stream().map(fv -> fv.getStringValue())
                            .collect(Collectors.toList()));
                }
            } else {
                switch (type.name()) {

                case "RECORD":
                    FieldValueList innerFieldsValue = value.getRecordValue();
                    FieldList innerFields = f.getSubFields();

                    Schema subSchema = tableSchema.getFields().stream().filter(field -> field.getName().equals(name))
                            .map(field -> Schema.of(field.getSubFields())).findFirst().get();
                    final FieldValueList innerFieldsValueWithSchema = FieldValueList.of(
                            StreamSupport.stream(innerFieldsValue.spliterator(), false).collect(Collectors.toList()),
                            subSchema.getFields());
                    Record.Builder innerRecordBuilder = recordBuilderFactoryService
                            .newRecordBuilder(convertToTckSchema(subSchema));

                    innerFields.stream().forEach(innerField -> convertToTckField(innerFieldsValueWithSchema, innerRecordBuilder,
                            innerField, subSchema));
                    rb.withRecord(name, innerRecordBuilder.build());
                    break;
                case "BOOLEAN":
                    rb.withBoolean(name, value.getBooleanValue());
                    break;
                case "BYTES":
                    rb.withBytes(name, Base64.decodeBase64(value.getStringValue()));
                    break;
                case "TIMESTAMP":
                    rb.withTimestamp(name, value.getTimestampValue() / 1000);
                    break;
                case "DATE":
                    try {
                        rb.withDateTime(name, new SimpleDateFormat("yyyy-MM-dd").parse(value.getStringValue()));
                    } catch (ParseException e) {
                        log.warn("Cannot parse date {}", value.getStringValue());
                    }
                    break;
                case "DATETIME":
                    try {
                        rb.withDateTime(name, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(value.getStringValue()));
                    } catch (ParseException e) {
                        log.warn("Cannot parse time {}", value.getStringValue());
                    }
                    break;
                case "FLOAT":
                    rb.withDouble(name, value.getDoubleValue());
                    break;
                case "INTEGER":
                    rb.withLong(name, value.getLongValue());
                    break;
                case "TIME":
                    try {
                        rb.withDateTime(name, new SimpleDateFormat("HH:mm:ss").parse(value.getStringValue()));
                    } catch (ParseException e) {
                        log.warn("Cannot parse time {}", value.getStringValue());
                    }
                    break;
                default:
                    rb.withString(name, value.getStringValue());
                }
            }
        }
    }

    private Gson createGsonBuilder() {
        JsonDeserializer<LegacySQLTypeName> typeDeserializer = (jsonElement, type, deserializationContext) -> {
            return LegacySQLTypeName.valueOf(jsonElement.getAsString());
        };

        JsonDeserializer<FieldList> subFieldsDeserializer = (jsonElement, type, deserializationContext) -> {
            Field[] fields = deserializationContext.deserialize(jsonElement.getAsJsonArray(), Field[].class);
            return FieldList.of(fields);
        };

        return new GsonBuilder().registerTypeAdapter(LegacySQLTypeName.class, typeDeserializer)
                .registerTypeAdapter(FieldList.class, subFieldsDeserializer).setLenient().create();
    }

    public GoogleCredentials getCredentials(String credentials) {
        try {
            return GoogleCredentials.fromStream(new ByteArrayInputStream(credentials.getBytes()))
                    .createScoped(BigqueryScopes.all());
        } catch (Exception e) {
            throw new BigQueryConnectorException(i18n.errorReadingCredentials(e.getMessage()), e);
        }
    }

    public void extractTable(BigQuery bigQuery, Table table, String blobGenericName) {
        ExtractJobConfiguration jobConfig = ExtractJobConfiguration.newBuilder(table.getTableId(), blobGenericName)
                .setFormat("Avro").build();

        JobInfo jobInfo = JobInfo.newBuilder(jobConfig).build();

        Job extractJob = bigQuery.create(jobInfo);
        try {
            extractJob.waitFor();
        } catch (InterruptedException e) {
            log.warn(e.getMessage());
        }
    }

}
