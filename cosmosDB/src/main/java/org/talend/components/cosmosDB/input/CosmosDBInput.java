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
package org.talend.components.cosmosDB.input;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.cosmosDB.service.CosmosDBService;
import org.talend.components.cosmosDB.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.util.Iterator;

@Slf4j
@Documentation("This component reads data from cosmosDB.")
public class CosmosDBInput implements Serializable {

    private I18nMessage i18n;

    private final CosmosDBInputConfiguration configuration;

    private final RecordBuilderFactory builderFactory;

    private CosmosDBService service;

    private DocumentClient client;

    final JsonToRecord jsonToRecord;

    InputParserFactory.IInputParser inputParser;

    public CosmosDBInput(@Option("configuration") final CosmosDBInputConfiguration configuration, final CosmosDBService service,
            final RecordBuilderFactory builderFactory, final I18nMessage i18n) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.i18n = i18n;
        this.jsonToRecord = new JsonToRecord(builderFactory);
    }

    @PostConstruct
    public void init() {
        client = service.documentClientFrom(configuration.getDataset().getDatastore());
        Iterator<Document> iterator = executeSimpleQuery(configuration.getDataset().getDatastore().getDatabaseID(),
                configuration.getDataset().getCollectionID());
        InputParserFactory inputParserFactory = new InputParserFactory(configuration.getDataset().getDocumentType(),
                builderFactory, iterator);
        this.inputParser = inputParserFactory.getInputParser();
    }

    @Producer
    public Record next() {
        return inputParser.get();
    }

    @PreDestroy
    public void release() {
        if (client != null) {
            client.close();
        }
    }

    private Iterator<Document> executeSimpleQuery(String databaseName, String collectionName) {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setPageSize(-1);
        queryOptions.setEnableCrossPartitionQuery(true);

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        final String query = configuration.getDataset().isUseQuery() ? configuration.getDataset().getQuery() : "SELECT * FROM c";
        FeedResponse<Document> queryResults = this.client.queryDocuments(collectionLink, query, queryOptions);
        log.info("Query [{}] execution success.", configuration.getDataset().getQuery());

        return queryResults.getQueryIterator();
    }
}