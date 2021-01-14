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
package org.talend.components.mongodb.sink;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.talend.components.common.stream.output.json.RecordToJson;
import org.talend.components.mongodb.BulkWriteType;
import org.talend.components.mongodb.KeyMapping;
import org.talend.components.mongodb.Mode;
import org.talend.components.mongodb.PathMapping;
import org.talend.components.mongodb.dataset.MongoDBReadAndWriteDataSet;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.components.mongodb.service.MongoDBService;
import org.talend.components.mongodb.service.RecordToDocument;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Version(1)
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "mongodb")
@Processor(name = "Sink")
@Documentation("This component writes data to MongoDB")
public class MongoDBProcessor implements Serializable {

    private I18nMessage i18n;

    private final MongoDBSinkConfiguration configuration;

    private final MongoDBService service;

    private transient MongoClient client;

    private transient MongoCollection<Document> collection;

    private transient RecordToJson recordToJson;

    private transient RecordToDocument recordToDocument;

    public MongoDBProcessor(@Option("configuration") final MongoDBSinkConfiguration configuration, final MongoDBService service,
            final I18nMessage i18n) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        this.recordToJson = new RecordToJson();
        this.recordToDocument = new RecordToDocument();

        MongoDBReadAndWriteDataSet dataset = configuration.getDataset();
        MongoDBDataStore datastore = dataset.getDatastore();
        client = service.createClient(datastore);
        MongoDatabase database = client.getDatabase(datastore.getDatabase());

        // apply to database level too, necessay?
        if (configuration.isSetWriteConcern()) {
            switch (configuration.getWriteConcern()) {
            case ACKNOWLEDGED:
                database = database.withWriteConcern(WriteConcern.ACKNOWLEDGED);
                break;
            case UNACKNOWLEDGED:
                database = database.withWriteConcern(WriteConcern.UNACKNOWLEDGED);
                break;
            case JOURNALED:
                database = database.withWriteConcern(WriteConcern.JOURNALED);
                break;
            case REPLICA_ACKNOWLEDGED:
                database = database.withWriteConcern(WriteConcern.REPLICA_ACKNOWLEDGED);
                break;
            }
        }

        collection = database.getCollection(dataset.getCollection());

        if (configuration.isSetWriteConcern()) {
            switch (configuration.getWriteConcern()) {
            case ACKNOWLEDGED:
                collection = collection.withWriteConcern(WriteConcern.ACKNOWLEDGED);
                break;
            case UNACKNOWLEDGED:
                collection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);
                break;
            case JOURNALED:
                collection = collection.withWriteConcern(WriteConcern.JOURNALED);
                break;
            case REPLICA_ACKNOWLEDGED:
                collection = collection.withWriteConcern(WriteConcern.REPLICA_ACKNOWLEDGED);
                break;
            }
        }

        writeModels = new ArrayList<>();
    }

    private transient List<WriteModel<Document>> writeModels;

    @BeforeGroup
    public void beforeGroup() {
        if (!configuration.isBulkWrite()) {
            return;
        }
        writeModels.clear();
    }

    @AfterGroup
    public void afterGroup() {
        if (!configuration.isBulkWrite()) {
            return;
        }
        if (writeModels != null && !writeModels.isEmpty()) {
            boolean ordered = configuration.getBulkWriteType() == BulkWriteType.ORDERED;
            collection.bulkWrite(writeModels, new BulkWriteOptions().ordered(ordered));
        }
    }

    private class DocumentGenerator {

        private Document document;

        private DocumentGenerator() {
            document = new Document();
        }

        void put(String parentNodePath, String curentName, Object value) {
            if (parentNodePath == null || "".equals(parentNodePath)) {
                document.put(curentName, value);
            } else {
                String objNames[] = parentNodePath.split("\\.");
                Document lastNode = getParentNode(parentNodePath, objNames.length - 1);
                lastNode.put(curentName, value);
                Document parentNode = null;
                for (int i = objNames.length - 1; i >= 0; i--) {
                    parentNode = getParentNode(parentNodePath, i - 1);
                    parentNode.put(objNames[i], lastNode);
                    lastNode = clone(parentNode);
                }
                document = lastNode;
            }
        }

        private Document clone(Document source) {
            Document to = new Document();
            for (java.util.Map.Entry<String, Object> cur : source.entrySet()) {
                to.append(cur.getKey(), cur.getValue());
            }
            return to;
        }

        public Document getParentNode(String parentNodePath, int index) {
            Document parentNode = document;
            if (parentNodePath == null || "".equals(parentNodePath)) {
                return document;
            } else {
                String objNames[] = parentNodePath.split("\\.");
                for (int i = 0; i <= index; i++) {
                    parentNode = (Document) parentNode.get(objNames[i]);
                    if (parentNode == null) {
                        parentNode = new Document();
                        return parentNode;
                    }
                    if (i == index) {
                        break;
                    }
                }
                return parentNode;
            }
        }

        Document getDocument() {
            return this.document;
        }
    }

    private Document getKeysQueryDocumentAndRemoveKeysFromSourceDocument(List<KeyMapping> keyMappings, Record record,
            Document document) {
        Document keysQueryDocument = new Document();
        if (keyMappings == null || keyMappings.isEmpty()) {
            throw new RuntimeException("need at least one key for set update/upsert action.");
        }
        for (KeyMapping keyMapping : configuration.getKeyMappings()) {
            String column = keyMapping.getColumn();
            String keyPath = keyMapping.getOriginElementPath();

            // TODO format it for right value format for lookup, now only follow the logic in studio,
            // so may not work for ObjectId, ISODate, NumberDecimal and so on, but they are not common as key
            // and "_id" can set in mongodb, not necessary as ObjectId type
            // in fact, record can be a tree like a json, not flat, but here we only suppose it's flat, not go deep
            if (configuration.getDataset().getMode() == Mode.TEXT) {
                // when TEXT mode, record is expected only have one column which contains the whole json content as text
                // so need to get it from document, not record
                Object value = getKeyValueFromDocumentAndRemoveKeys(document, column);
                keysQueryDocument.put(isEmpty(keyPath) ? column : keyPath, value);
            } else {
                Object value = getKeyValueFromRecord(record, column);
                getKeyValueFromDocumentAndRemoveKeys(document, column);
                keysQueryDocument.put(isEmpty(keyPath) ? column : keyPath, value);
            }
        }
        return keysQueryDocument;
    }

    // only support path like a.b.c, not support array
    private Object getKeyValueFromDocumentAndRemoveKeys(Document document, String keyColumnPath) {
        if (isEmpty(keyColumnPath)) {
            throw new RuntimeException("Please set the key column for update or upsert.");
        }

        String[] paths = keyColumnPath.split("\\.");
        Object result = null;
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];

            if (isEmpty(path)) {
                throw new RuntimeException("Please set the right key column for update or upsert.");
            }

            if (document != null) {
                Object v = document.get(path);
                if (v instanceof Document) {
                    document = (Document) v;
                } else if (i == (paths.length - 1)) {
                    result = v;
                    // need to remove origin key-value for update and upsert
                    // https://jira.talendforge.org/browse/TDI-44003
                    document.remove(path);
                }
            } else {
                break;
            }
        }

        return result;
    }

    // only support path like a.b.c, not support array
    private Object getKeyValueFromRecord(Record record, String keyColumnPath) {
        if (isEmpty(keyColumnPath)) {
            throw new RuntimeException("Please set the key column for update or upsert.");
        }

        String[] paths = keyColumnPath.split("\\.");
        Object result = null;
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];

            if (isEmpty(path)) {
                throw new RuntimeException("Please set the right key column for update or upsert.");
            }

            if (record != null) {
                Object v = record.get(Object.class, path);
                if (v instanceof Record) {
                    record = (Record) v;
                } else if (i == (paths.length - 1)) {
                    result = v;
                }
            } else {
                break;
            }
        }

        return result;
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    @ElementListener
    public void onNext(@Input final Record record) {
        if (configuration.getDataset().getMode() == Mode.TEXT) {
            // we store the whole document here as a string
            String uniqueFieldName = record.getSchema().getEntries().get(0).getName();
            Document document = null;
            try {
                String value = record.getString(uniqueFieldName);
                document = Document.parse(value);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(
                        "the input record is no valid for TEXT mode, the record's first column must be a json format text.");
            }

            doDataAction(record, document);
        } else if (configuration.getDataset().getMode() == Mode.JSON) {
            Document document = convertRecord2DocumentDirectly(record);

            doDataAction(record, document);
        } else {
            /*
             * DocumentGenerator dg = new DocumentGenerator();
             * List<PathMapping> mappings = configuration.getDataset().getPathMappings();
             * Map<String, PathMapping> inputFieldName2PathMapping = new LinkedHashMap<>();
             * 
             * // TODO now only use name mapping, improve it with index mapping
             * for (PathMapping mapping : mappings) {
             * String column = mapping.getColumn();
             * inputFieldName2PathMapping.put(column, mapping);
             * }
             * 
             * for (Schema.Entry entry : record.getSchema().getEntries()) {// schema from input
             * PathMapping mapping = inputFieldName2PathMapping.get(entry.getName());
             * String originElement = mapping.getOriginElement();
             * dg.put(mapping.getParentNodePath(), originElement != null ? originElement : entry.getName(),
             * record.get(Object.class, entry.getName()));
             * }
             * 
             * doDataAction(record, dg.getDocument());
             */
        }
    }

    private Document convertRecord2Document(@Input Record record) {
        JsonObject jsonObject = this.recordToJson.fromRecord(record);
        String jsonContent = jsonObject.toString();
        return Document.parse(jsonContent);
    }

    private Document convertRecord2DocumentDirectly(@Input Record record) {
        return recordToDocument.fromRecord(record);
    }

    private void doDataAction(@Input Record record, Document document) {
        switch (configuration.getDataAction()) {
        case INSERT:
            if (configuration.isBulkWrite()) {
                writeModels.add(new InsertOneModel(document));
            } else {
                collection.insertOne(document);
            }
            break;
        case SET:
            if (configuration.isBulkWrite()) {
                if (configuration.isUpdateAllDocuments()) {
                    writeModels.add(new UpdateManyModel<Document>(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document)));
                } else {
                    writeModels.add(new UpdateOneModel<Document>(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document)));
                }
            } else {
                if (configuration.isUpdateAllDocuments()) {
                    collection.updateMany(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document));
                } else {
                    collection.updateOne(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document));
                }
            }
            break;
        case UPSERT_WITH_SET:
            // though mongodb support to set "_id" key self, not auto-generate, but when do upsert with "_id" or other key, it
            // mean if not match, should do insert,
            // but mongo here will throw : Performing an update on the path '_id' would modify the immutable field '_id', i think
            // it's a limit of mongodb as i did't change the value of "_id"
            // why update can works, upsert not work? As not match, should insert, i can't just remove "_id" column to make it
            // right, as that is not expected as lose "_id", and auto-generated when insert.
            // TODO show a more clear exception here
            if (configuration.isBulkWrite()) {
                if (configuration.isUpdateAllDocuments()) {
                    writeModels.add(new UpdateManyModel<Document>(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document), new UpdateOptions().upsert(true)));
                } else {
                    writeModels.add(new UpdateOneModel<Document>(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document), new UpdateOptions().upsert(true)));
                }
            } else {
                if (configuration.isUpdateAllDocuments()) {
                    collection.updateMany(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document), new UpdateOptions().upsert(true));
                } else {
                    collection.updateOne(
                            getKeysQueryDocumentAndRemoveKeysFromSourceDocument(configuration.getKeyMappings(), record, document),
                            new Document("$set", document), new UpdateOptions().upsert(true));
                }
            }
            break;
        }
    }

    @PreDestroy
    public void release() {
        service.closeClient(client);
    }

    // copy from couchbase, not use now, will use it maybe
    private Object jsonValueFromRecordValue(Schema.Entry entry, Record record) {
        String entryName = entry.getName();
        Object value = record.get(Object.class, entryName);
        if (null == value) {
            // TODO check use what explain null
            return "";
        }
        switch (entry.getType()) {
        case INT:
            return record.getInt(entryName);
        case LONG:
            return record.getLong(entryName);
        case BYTES:
            return java.util.Base64.getEncoder().encodeToString(record.getBytes(entryName));
        case FLOAT:
            return Double.parseDouble(String.valueOf(record.getFloat(entryName)));
        case DOUBLE:
            return record.getDouble(entryName);
        case STRING:
            return createJsonFromString(record.getString(entryName));
        case BOOLEAN:
            return record.getBoolean(entryName);
        case ARRAY:
            return record.getArray(List.class, entryName);
        case DATETIME:
            return record.getDateTime(entryName).toString();
        case RECORD:
            return record.getRecord(entryName);
        default:
            throw new IllegalArgumentException("Unknown Type " + entry.getType());
        }
    }

    private Object createJsonFromString(String str) {
        Object value = null;
        try {
            value = Document.parse(str);
        } catch (Exception e) {
            // can't create JSON object from String ignore exception
            // and try to create JSON array
        } finally {
            if (value != null)
                return value;
        }
        // TODO consider array case
        /*
         * try {
         * value = Document.fromArray(str);
         * } catch (Exception e) {
         * value = str;
         * }
         */
        return value;
    }

}
