/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

package org.talend.components.mongodb.source;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import org.talend.components.mongodb.service.MongoDBService;

import static org.talend.sdk.component.api.record.Schema.Type.ARRAY;
import static org.talend.sdk.component.api.record.Schema.Type.RECORD;

@Slf4j
@Documentation("Input source for MongoDB")
public class MongoDBInputSource implements Serializable {

    private final MongoDBInputMapperConfiguration configuration;

    private final MongoDBService service;

    private final RecordBuilderFactory builderFactory;

    private MongoClient mongoClient;

    private MongoCursor<Document> cursor;

    private Schema schema;

    private Map<String, String> pathMap = new HashMap<>();

    private final List<String> columnsList = new ArrayList<>();

    private final QueryDataFinder<Document> dataFinder;

    private final I18nMessage i18nMessage;

    public MongoDBInputSource(@Option("configuration") final MongoDBInputMapperConfiguration configuration,
            final MongoDBService service, final RecordBuilderFactory builderFactory, final I18nMessage i18nMessage) {
        this.i18nMessage = i18nMessage;
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.dataFinder = configuration.getQueryType() == MongoDBInputMapperConfiguration.QueryType.FIND_QUERY
                ? new FindQueryDataFinder()
                : new AggregationPipelineQueryDataFinder();
    }

    @PostConstruct
    public void init() {
        mongoClient = service.getMongoClient(configuration.getDataset().getDatastore(),
                new InputClientOptionsFactory(configuration, i18nMessage));
        MongoCollection<Document> collection = service.getCollection(configuration.getDataset(), mongoClient);

        checkIndexList(collection);

        pathMap = parsePathMap(configuration.getMapping());
        if (configuration.getDataset().getSchema() != null && !configuration.getDataset().getSchema().isEmpty()) {
            columnsList.addAll(configuration.getDataset().getSchema());
        }
        cursor = dataFinder.findData(collection, configuration);
    }

    // Do we still need this? it will be run on cloud, not sure anyone would pay attention to this warning.
    private void checkIndexList(MongoCollection<?> collection) {
        boolean needIndexWarning = true;
        String indexList = "";
        for (DBObject index : collection.listIndexes(DBObject.class)) {
            for (String key : ((com.mongodb.DBObject) index.get("key")).keySet()) {
                // The regexp is:
                // - contain the db DBcolumnName between two backslashed quotes
                // - is followed at some point by a colon
                // - there is no comma between the the DBcolumnName and the colon
                if (configuration.getQuery().matches(".*" + key + "[^,]*:.*")) {
                    // We have an index, do not print error message
                    needIndexWarning = false;
                } else {
                    // This index is not in the query, add it into the indexList
                    indexList += ", " + key;
                }
            }

        }
        if ((!"".equals(indexList)) && (needIndexWarning)) {
            log.warn("tMongoDBInput_1 - The query does not contain any reference an index.  [" + indexList.substring(1) + " ]");
        }
    }

    private final Map<String, String> parsePathMap(List<InputMapping> mapping) {
        if (mapping == null || mapping.isEmpty()) {
            return Collections.emptyMap();
        }
        return mapping.stream().collect(Collectors.toMap(InputMapping::getColumn, InputMapping::getParentNodePath));
    }

    @Producer
    public Record next() {
        if (cursor.hasNext()) {
            Document document = cursor.next();
            if (columnsList.isEmpty()) {
                columnsList.addAll(document.keySet());
            }
            if (schema == null) {
                schema = parseSchema(document);
            }
            final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
            schema.getEntries().stream().forEach(
                    entry -> addColumn(recordBuilder, entry, getValue(pathMap.get(entry.getName()), entry.getName(), document)));
            return recordBuilder.build();
        }
        return null;
    }

    private Schema parseSchema(Document document) {
        final Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(RECORD);
        columnsList.stream().forEach(name -> addField(schemaBuilder, name, getValue(pathMap.get(name), name, document)));
        return schemaBuilder.build();
    }

    private void addField(final Schema.Builder schemaBuilder, final String name, Object value) {
        if (value == null) {
            log.warn(i18nMessage.schemaFieldParseError(name));
            return;
        }
        final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        Schema.Type type = getSchemaType(value);
        entryBuilder.withName(name).withNullable(true).withType(type);
        if (type == ARRAY) {
            List<?> listValue = (List<?>) value;
            Object listObject = (listValue == null || listValue.isEmpty()) ? null : listValue.get(0);
            entryBuilder.withElementSchema(builderFactory.newSchemaBuilder(getSchemaType(listObject)).build());
        }
        schemaBuilder.withEntry(entryBuilder.build());
    }

    private void addColumn(final Record.Builder builder, final Schema.Entry entry, Object value) {
        final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        Schema.Type type = entry.getType();
        entryBuilder.withName(entry.getName()).withNullable(true).withType(type);
        switch (type) {
        case ARRAY:
            List<?> listValue = (List<?>) value;
            entryBuilder.withElementSchema(entry.getElementSchema());
            builder.withArray(entryBuilder.build(), listValue);
            break;
        case FLOAT:
            builder.withFloat(entryBuilder.build(), value == null ? null : (Float) value);
            break;
        case DOUBLE:
            builder.withDouble(entryBuilder.build(), value == null ? null : (Double) value);
            break;
        case BYTES:
            builder.withBytes(entryBuilder.build(), value == null ? null : ((Binary) value).getData());
            break;
        case STRING:
            builder.withString(entryBuilder.build(), value == null ? null : String.valueOf(value));
            break;
        case LONG:
            builder.withLong(entryBuilder.build(), value == null ? null : (Long) value);
            break;
        case INT:
            builder.withInt(entryBuilder.build(), value == null ? null : (Integer) value);
            break;
        case DATETIME:
            builder.withDateTime(entryBuilder.build(), value == null ? null : (Date) value);
            break;
        case BOOLEAN:
            builder.withBoolean(entryBuilder.build(), value == null ? null : (Boolean) value);
            break;
        }
    }

    private Schema.Type getSchemaType(Object value) {
        if (value instanceof ObjectId) {
            return Schema.Type.STRING;
        } else if (value instanceof String) {
            return Schema.Type.STRING;
        } else if (value instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        } else if (value instanceof Date) {
            return Schema.Type.DATETIME;
        } else if (value instanceof Double) {
            return Schema.Type.DOUBLE;
        } else if (value instanceof Integer) {
            return Schema.Type.INT;
        } else if (value instanceof Long) {
            return Schema.Type.LONG;
        } else if (value instanceof Binary) {
            return Schema.Type.BYTES;
        } else if (value instanceof List<?>) {
            return Schema.Type.ARRAY;
        } else {
            return Schema.Type.STRING;
        }
    }

    public Object getValue(String parentNode, String currentName, Document dbObject) {
        Object value = null;
        if (dbObject == null) {
            return null;
        }
        if (parentNode == null || "".equals(parentNode)) {
            if ("*".equals(currentName)) {
                value = dbObject;
            } else if (dbObject.get(currentName) != null) {
                value = dbObject.get(currentName);
            }
        } else {
            String objNames[] = parentNode.split("\\.");
            org.bson.Document currentObj = dbObject;
            for (int i = 0; i < objNames.length; i++) {
                currentObj = (org.bson.Document) currentObj.get(objNames[i]);
                if (currentObj == null) {
                    break;
                }
            }
            if ("*".equals(currentName)) {
                value = currentObj;
            } else if (currentObj != null) {
                value = currentObj.get(currentName);
            }
        }
        return value;
    }

    @PreDestroy
    public void release() {
        mongoClient.close();
    }
}