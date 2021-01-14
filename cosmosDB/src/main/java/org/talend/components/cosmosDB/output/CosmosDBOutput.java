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

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.cosmosDB.service.CosmosDBService;
import org.talend.components.cosmosDB.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RequestOptions;

import lombok.extern.slf4j.Slf4j;

@Version(2)
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "CosmosDBOutput")
@Processor(name = "SQLAPIOutput")
@Documentation("This component writes data to cosmosDB")
public class CosmosDBOutput implements Serializable {

    private I18nMessage i18n;

    private final CosmosDBOutputConfiguration configuration;

    private final CosmosDBService service;

    private transient DocumentClient client;

    private OutputParserFactory.IOutputParser out;

    public CosmosDBOutput(@Option("configuration") final CosmosDBOutputConfiguration configuration, final CosmosDBService service,
            final I18nMessage i18n) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {

        client = service.documentClientFrom(configuration.getDataset().getDatastore());

        if (configuration.isCreateCollection()) {
            createDocumentCollectionIfNotExists();
        }
        out = new OutputParserFactory(configuration, client).getOutputParser();
    }

    @ElementListener
    public void onNext(@Input final Record record) {
        out.output(record);
    }

    @PreDestroy
    public void release() {
        if (client != null) {
            client.close();
        }
    }

    private void createDocumentCollectionIfNotExists() {
        final String databaseName = configuration.getDataset().getDatastore().getDatabaseID();
        final String collectionName = configuration.getDataset().getCollectionID();
        String databaseLink = String.format("/dbs/%s", databaseName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        try {
            this.client.readCollection(collectionLink, null);
            log.info(String.format("Found %s", collectionName));
        } catch (DocumentClientException de) {
            // If the document collection does not exist, create a new
            // collection
            if (de.getStatusCode() == 404) {
                if (configuration.getDataAction() == DataAction.DELETE || configuration.getDataAction() == DataAction.UPDATE) {
                    throw new IllegalArgumentException(de);
                }
                DocumentCollection collectionInfo = new DocumentCollection();
                collectionInfo.setId(collectionName);

                // Optionally, you can configure the indexing policy of a
                // collection. Here we configure collections for maximum query
                // flexibility including string range queries.
                RangeIndex index = new RangeIndex(DataType.String);
                index.setPrecision(-1);

                collectionInfo.setIndexingPolicy(new IndexingPolicy(new Index[] { index }));
                if (StringUtils.isNotEmpty(configuration.getPartitionKey())) {
                    PartitionKeyDefinition pkd = new PartitionKeyDefinition();
                    pkd.setPaths(Arrays.asList(configuration.getPartitionKey().split(",")));
                    collectionInfo.setPartitionKey(pkd);
                }
                // DocumentDB collections can be reserved with throughput
                // specified in request units/second. 1 RU is a normalized
                // request equivalent to the read of a 1KB document. Here we
                // create a collection with 400 RU/s.
                RequestOptions requestOptions = new RequestOptions();
                if (configuration.getOfferThroughput() > 0) {
                    requestOptions.setOfferThroughput(configuration.getOfferThroughput());
                }

                try {
                    this.client.createCollection(databaseLink, collectionInfo, requestOptions);
                } catch (DocumentClientException e) {
                    throw new IllegalArgumentException(e);
                }

                log.info(String.format("Created %s", collectionName));
            } else {
                throw new IllegalArgumentException(de);
            }
        }
    }

}
