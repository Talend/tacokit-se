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
package org.talend.components.cosmosDB;

import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class CosmosTestUtils {

    private String databaseID;

    private String collectionID;

    DocumentClient client;

    public CosmosTestUtils(DocumentClient client, String databaseID, String collectionID) {
        this.databaseID = databaseID;
        this.collectionID = collectionID;
        this.client = client;
    }

    public void createDatabaseIfNotExists() throws DocumentClientException {
        String databaseLink = String.format("/dbs/%s", databaseID);

        // Check to verify a database with the id=FamilyDB does not exist
        try {
            client.readDatabase(databaseLink, null);

        } catch (DocumentClientException de) {
            // If the database does not exist, create a new database
            if (de.getStatusCode() == 404) {
                Database database = new Database();
                database.setId(databaseID);
                client.createDatabase(database, null);
            } else {
                throw de;
            }
        }
    }

    public boolean isCollectionExist(String collectionID) throws DocumentClientException {
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseID, collectionID);
        try {
            client.readCollection(collectionLink, null);
        } catch (DocumentClientException e) {
            if (e.getStatusCode() == 404) {
                log.warn("Collection [" + collectionID + "] does not exist.");
                return false;
            } else {
                throw e;
            }
        }
        return true;
    }

    public void createDocumentCollectionIfNotExists() throws IOException, DocumentClientException {
        String databaseLink = String.format("/dbs/%s", databaseID);
        if (!isCollectionExist(collectionID)) {
            log.info("Collection [" + collectionID + "] will be created.");
            DocumentCollection collectionInfo = new DocumentCollection();
            collectionInfo.setId(collectionID);
            RangeIndex index = new RangeIndex(DataType.String);
            index.setPrecision(-1);
            collectionInfo.setIndexingPolicy(new IndexingPolicy(new Index[] { index }));
            PartitionKeyDefinition pkd = new PartitionKeyDefinition();
            pkd.setPaths(Arrays.asList("/lastName"));
            collectionInfo.setPartitionKey(pkd);
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setOfferThroughput(400);
            client.createCollection(databaseLink, collectionInfo, requestOptions);
            log.info("Collection [" + collectionID + "] created.");
        }

    }

    public Document readDocuments(String collectionID, String documentID, String partitionKey) throws DocumentClientException {
        String collectionLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseID, collectionID, documentID);
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(partitionKey));
        requestOptions.setOfferThroughput(400);
        ResourceResponse<Document> documentResourceResponse = client.readDocument(collectionLink, requestOptions);
        return documentResourceResponse.getResource();
    }

    public void deleteCollection(String collectionID) throws DocumentClientException {
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseID, collectionID);
        try {
            client.deleteCollection(collectionLink, null);
        } catch (DocumentClientException e) {
            log.error("There's a problem when delete ccollection [" + collectionID + "] , please delete it manually.");
            throw e;
        }
    }

    public void deleteCollection() throws DocumentClientException {
        deleteCollection(collectionID);
    }

    public void insertDocument(String json) {
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseID, collectionID);
        try {
            Document document = new Document(json);
            client.createDocument(collectionLink, document, new RequestOptions(), true);
        } catch (DocumentClientException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public void dropDatabase() throws DocumentClientException {
        try {
            client.deleteDatabase("/dbs/" + databaseID, null);
        } catch (DocumentClientException e) {
            if (!(e.getStatusCode() == 404)) {
                log.error("Cannot Drop database: [" + databaseID + "] please deleted manually");
                this.deleteCollection();
                throw e;
            }
        } finally {
            client.close();
        }
    }
}
