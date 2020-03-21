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
package org.talend.components.cosmosDB.output;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RequestOptions;
import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.record.Record;

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
            return delegate.substring(11, delegate.length() - 1);
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

        @Override
        public void output(Record record) {
            String id = record.getString(configuration.getIdFieldName());
            final String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, id);
            try {
                client.deleteDocument(documentLink, null);
            } catch (DocumentClientException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    class Update implements IOutputParser {

        @Override
        public void output(Record record) {
            String id = record.getString(configuration.getIdFieldName());
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

        @Override
        public void output(Record record) {
            String id = record.getString(configuration.getIdFieldName());
            final String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, id);
            String jsonString = getJsonString(record);
            try {
                client.upsertDocument(documentLink, new Document(jsonString), new RequestOptions(), disAbleautoID);
            } catch (DocumentClientException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
