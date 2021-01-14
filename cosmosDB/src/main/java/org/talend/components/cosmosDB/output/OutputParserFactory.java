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
package org.talend.components.cosmosDB.output;

import org.apache.commons.lang3.StringUtils;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OutputParserFactory {

    final String databaseName;

    final String collectionName;

    final CosmosDBOutputConfiguration configuration;

    DocumentClient client;

    public OutputParserFactory(final CosmosDBOutputConfiguration configuration, DocumentClient client) {
        this.configuration = configuration;
        this.client = client;
        databaseName = configuration.getDataset().getDatastore().getDatabaseID();
        collectionName = configuration.getDataset().getCollectionID();
    }

    public IOutputParser getOutputParser() {
        switch (configuration.getDataAction()) {
        case INSERT:
            return new Insert();
        case DELETE:
            return new Delete();
        case UPDATE:
            return new Update();
        case UPSERT:
            return new Upsert();
        default:
            return null;
        }
    }

    public String getJsonString(Record record) {
        String delegate = record.toString();
        log.debug("delegate: " + delegate);
        if (delegate.startsWith("AvroRecord")) {
            // To avoid import dependence of AvroRecord
            return delegate.substring(20, delegate.length() - 1);
        }
        return delegate;
    }

    interface IOutputParser {

        void output(Record record);
    }

    class Insert implements IOutputParser {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        boolean disAbleautoID = !configuration.isAutoIDGeneration();

        @Override
        public void output(Record record) {
            String jsonString = getJsonString(record);
            try {
                Document document = new Document(jsonString);
                client.createDocument(collectionLink, document, new RequestOptions(), disAbleautoID);
            } catch (DocumentClientException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    class Delete implements IOutputParser {

        String partitionKey;

        Delete() {
            String partitionKeyForDelete = configuration.getPartitionKeyForDelete();
            if (StringUtils.isNotEmpty(partitionKeyForDelete)) {
                partitionKey = partitionKeyForDelete.startsWith("/") ? partitionKeyForDelete.substring(1) : partitionKeyForDelete;
            }
        }

        @Override
        public void output(Record record) {
            String id = record.getString("id");
            final String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, id);
            try {
                client.deleteDocument(documentLink, getPartitionKey(record));
            } catch (DocumentClientException e) {
                throw new IllegalArgumentException(e);
            }
        }

        public RequestOptions getPartitionKey(Record record) {
            RequestOptions requestOptions = null;
            if (StringUtils.isNotEmpty(partitionKey)) {
                // TODO support complex partition key
                requestOptions = new RequestOptions();
                requestOptions.setPartitionKey(new PartitionKey(record.get(Object.class, partitionKey)));

            }
            return requestOptions;
        }
    }

    class Update implements IOutputParser {

        @Override
        public void output(Record record) {
            String id = record.getString("id");
            final String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, id);
            String jsonString = getJsonString(record);
            try {
                client.replaceDocument(documentLink, new Document(jsonString), new RequestOptions());
            } catch (DocumentClientException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    class Upsert implements IOutputParser {

        boolean disAbleautoID = !configuration.isAutoIDGeneration();

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        @Override
        public void output(Record record) {
            String jsonString = getJsonString(record);
            try {
                client.upsertDocument(collectionLink, new Document(jsonString), new RequestOptions(), disAbleautoID);
            } catch (DocumentClientException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
