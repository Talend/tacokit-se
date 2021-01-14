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
package org.talend.components.couchbase.output;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.components.couchbase.service.CouchbaseService;
import org.talend.components.couchbase.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.subdoc.MutateInBuilder;

import lombok.extern.slf4j.Slf4j;

@Version(1)
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "CouchbaseOutput")
@Processor(name = "Output")
@Documentation("This component writes data to Couchbase")
public class CouchbaseOutput implements Serializable {

    private I18nMessage i18n;

    private Bucket bucket;

    private String idFieldName;

    private final CouchbaseOutputConfiguration configuration;

    private final CouchbaseService service;

    private static final String CONTENT_FIELD_NAME = "content";

    public CouchbaseOutput(@Option("configuration") final CouchbaseOutputConfiguration configuration,
            final CouchbaseService service, final I18nMessage i18n) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        Cluster cluster = service.openConnection(configuration.getDataSet().getDatastore());
        bucket = service.openBucket(cluster, configuration.getDataSet().getBucket());
        idFieldName = configuration.getIdFieldName();
    }

    @ElementListener
    public void onNext(@Input final Record record) {
        if (configuration.isUseN1QLQuery()) {
            Map<String, String> mappings = configuration.getQueryParams().stream()
                    .collect(Collectors.toMap(N1QLQueryParameter::getColumn, N1QLQueryParameter::getQueryParameterName));
            JsonObject namedParams = buildJsonObject(record, mappings);
            final N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(configuration.getQuery(), namedParams));
            if (!queryResult.finalSuccess()) {
                final String errors = queryResult.errors().stream()
                        .map(error -> String.format("[%d] %s", error.getInt("code"), error.getString("msg")))
                        .collect(Collectors.joining("\n"));
                log.error("N1QL failed: {}.", errors);
                throw new RuntimeException(errors);
            }
        } else {
            if (configuration.isPartialUpdate()) {
                updatePartiallyDocument(record);
            } else {
                if (configuration.getDataSet().getDocumentType() == DocumentType.BINARY) {
                    bucket.upsert(toBinaryDocument(idFieldName, record));
                } else if (configuration.getDataSet().getDocumentType() == DocumentType.STRING) {
                    bucket.upsert(toStringDocument(idFieldName, record));
                } else {
                    bucket.upsert(toJsonDocument(idFieldName, record));
                }
            }
        }
    }

    @PreDestroy
    public void release() {
        service.closeBucket(bucket);
        service.closeConnection(configuration.getDataSet().getDatastore());
    }

    private BinaryDocument toBinaryDocument(String idFieldName, Record record) {
        ByteBuf toWrite = Unpooled.copiedBuffer(record.getBytes(CONTENT_FIELD_NAME));
        return BinaryDocument.create(record.getString(idFieldName), toWrite);
    }

    private StringDocument toStringDocument(String idFieldName, Record record) {
        String content = record.getString(CONTENT_FIELD_NAME);
        return StringDocument.create(record.getString(idFieldName), content);
    }

    private void updatePartiallyDocument(Record record) {
        final MutateInBuilder[] mutateBuilder = { bucket.mutateIn(record.getString(idFieldName)) };
        record.getSchema().getEntries().stream().filter(e -> !idFieldName.equals(e.getName()))
                .forEach(e -> mutateBuilder[0] = mutateBuilder[0].upsert(e.getName(), jsonValueFromRecordValue(e, record)));
        mutateBuilder[0].execute();
    }

    private Object jsonValueFromRecordValue(Schema.Entry entry, Record record) {
        String entryName = entry.getName();
        Object value = record.get(Object.class, entryName);
        if (null == value) {
            return null;
        }
        switch (entry.getType()) {
        case INT:
            return record.getInt(entryName);
        case LONG:
            return record.getLong(entryName);
        case BYTES:
            return com.couchbase.client.core.utils.Base64.encode(record.getBytes(entryName));
        case FLOAT:
            return Double.parseDouble(String.valueOf(record.getFloat(entryName)));
        case DOUBLE:
            return record.getDouble(entryName);
        case STRING:
            return createJsonFromString(record.getString(entryName));
        case BOOLEAN:
            return record.getBoolean(entryName);
        case ARRAY:
            return JsonArray.from((List<?>) record.getArray(List.class, entryName));
        case DATETIME:
            return record.getDateTime(entryName).toString();
        case RECORD:
            return record.getRecord(entryName);
        default:
            throw new IllegalArgumentException("Unknown Type " + entry.getType());
        }
    }

    private JsonObject buildJsonObject(Record record, Map<String, String> mappings) {
        JsonObject jsonObject = JsonObject.create();
        record.getSchema().getEntries().stream().forEach(entry -> {
            String property = mappings.getOrDefault(entry.getName(), entry.getName());
            Object value = jsonValueFromRecordValue(entry, record);
            jsonObject.put(property, value);
        });
        return jsonObject;
    }

    /**
     * Calls {@link #buildJsonObject(Record, Map)} and then removes the KEY(idFieldName) from it
     *
     * @param record
     * @return JsonObject
     */
    private JsonObject buildJsonObjectWithoutId(Record record) {
        return buildJsonObject(record, Collections.emptyMap()).removeKey(idFieldName);
    }

    public JsonDocument toJsonDocument(String idFieldName, Record record) {
        return JsonDocument.create(record.getString(idFieldName), buildJsonObjectWithoutId(record));
    }

    private Object createJsonFromString(String str) {
        Object value = null;
        try {
            value = JsonObject.fromJson(str);
        } catch (Exception e) {
            // can't create JSON object from String ignore exception
            // and try to create JSON array
        } finally {
            if (value != null)
                return value;
        }
        try {
            value = JsonArray.fromJson(str);
        } catch (Exception e) {
            value = str;
        }
        return value;
    }

}
