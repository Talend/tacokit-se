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
package org.talend.components.mongodb.source;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.talend.components.mongodb.dataset.BaseDataSet;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.components.mongodb.service.MongoDBService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "mongodb")
@PartitionMapper(name = "CollectionQuerySource")
@Documentation("MongoDB Source with query")
@Slf4j
public class MongoDBQueryMapper implements Serializable {

    private final MongoDBQuerySourceConfiguration configuration;

    private final MongoDBService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private final I18nMessage i18nMessage;

    @Setter
    private String query4Split;

    public MongoDBQueryMapper(@Option("configuration") final MongoDBQuerySourceConfiguration configuration,
            final MongoDBService service, final RecordBuilderFactory recordBuilderFactory, final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18nMessage = i18nMessage;
    }

    @Assessor
    public long estimateSize() {
        if (!SplitUtil.isSplit(configuration.getDataset().getQuery(), configuration.getSampleLimit())) {
            return 1l;
        }

        return SplitUtil.getEstimatedSizeBytes(configuration, service);
    }

    @Split
    public List<MongoDBQueryMapper> split(@PartitionSize final long bundles) {
        if (!SplitUtil.isSplit(configuration.getDataset().getQuery(), configuration.getSampleLimit())) {
            return singletonList(this);
        }

        if (bundles < 2) {
            return singletonList(this);
        }

        int splitCount = (int) (estimateSize() / bundles);

        log.info("split number : " + splitCount + ",  the size of every split from platform : " + bundles);

        if (splitCount > 1) {
            List<String> queries4Split = SplitUtil.getQueries4Split(configuration, service, splitCount);
            if (queries4Split == null || queries4Split.size() < 2) {
                return singletonList(this);
            }

            return queries4Split.stream().map(query4Split -> cloneMapperAndSetSplitParameter4Reader(query4Split))
                    .collect(Collectors.toList());
        }

        return singletonList(this);
    }

    private MongoDBQueryMapper cloneMapperAndSetSplitParameter4Reader(String query4Split) {
        MongoDBQueryMapper mapper = new MongoDBQueryMapper(configuration, service, recordBuilderFactory, i18nMessage);
        mapper.setQuery4Split(query4Split);
        return mapper;
    }

    @Emitter
    public MongoDBReader createWorker() {
        // here we create an actual worker,
        // you are free to rework the configuration etc but our default generated implementation
        // propagates the partition mapper entries.
        return new MongoDBReader(configuration, service, recordBuilderFactory, i18nMessage, query4Split);
    }
}