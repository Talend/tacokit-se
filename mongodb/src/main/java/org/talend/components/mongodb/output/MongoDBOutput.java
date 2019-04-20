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

package org.talend.components.mongodb.output;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.talend.components.mongodb.output.processor.ValuesProcessor;
import org.talend.components.mongodb.output.processor.ValuesProcessorsFactory;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;

import org.talend.components.mongodb.service.MongoDBService;
import org.talend.sdk.component.api.record.Schema;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "MongoDBOutput")
@Processor(name = "MongoDBOutput")
@Documentation("MongoDB output component")
public class MongoDBOutput implements Serializable {

    private final I18nMessage i18n;

    private final MongoDBOutputConfiguration configuration;

    private MongoCollection<Document> collection;

    private Map<String, OutputMapping> mapping;

    private List<String> columnsList = new ArrayList<>();

    private final MongoDBService service;

    private MongoClient client;

    private ValuesProcessor<? extends WriteModel<Document>> valuesProcessor;

    public MongoDBOutput(@Option("configuration") final MongoDBOutputConfiguration configuration, final MongoDBService service,
            final I18nMessage i18n) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        this.client = service.getMongoClient(configuration.getDataset().getDatastore(),
                new OutputClientOptionsFactory(configuration, i18n));
        collection = service.getCollection(configuration.getDataset(), client);
        // if (configuration.isDropCollectionIfExists()) {
        // collection.drop();
        // }
        List<OutputMapping> outputMappings = configuration.getOutputMappingList();
        if (outputMappings != null) {
            mapping = outputMappings.stream().collect(Collectors.toMap(OutputMapping::getColumn, c -> c));
        } else {
            mapping = Collections.emptyMap();
        }
        if (configuration.getDataset().getSchema() != null) {
            columnsList.addAll(configuration.getDataset().getSchema());
        }
        valuesProcessor = ValuesProcessorsFactory.createProcessor(configuration, collection, i18n);
    }

    @BeforeGroup
    public void beforeGroup() {
    }

    @ElementListener
    public void onNext(@Input final Record defaultInput) {
        if (columnsList.isEmpty()) {
            columnsList.addAll(
                    defaultInput.getSchema().getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList()));
        }
        final Map<String, Schema.Type> defaultInputTypesMap = defaultInput.getSchema().getEntries().stream()
                .collect(Collectors.toMap(Schema.Entry::getName, Schema.Entry::getType));
        columnsList.stream().forEach(col -> valuesProcessor.processField(mapping.get(col), col,
                getValue(defaultInput, col, defaultInputTypesMap.get(col))));
        valuesProcessor.finalizeRecord();
    }

    @AfterGroup
    public void afterGroup() {
        valuesProcessor.flush();
    }

    @PreDestroy
    public void release() {
        valuesProcessor.close();
        client.close();
    }

    protected Object getValue(Record defaultInput, String col, Schema.Type type) {
        Object value = null;
        if (type == null) {
            value = defaultInput.get(Object.class, col);
        } else {
            switch (type) {
            case INT:
                value = defaultInput.getInt(col);
                break;
            case LONG:
                value = defaultInput.getLong(col);
                break;
            case RECORD:
                break;
            case ARRAY:
                value = defaultInput.getArray(Object.class, col);
                break;
            case STRING:
                value = defaultInput.getString(col);
                break;
            case BYTES:
                value = defaultInput.getBytes(col);
                break;
            case FLOAT:
                value = defaultInput.getFloat(col);
                break;
            case DOUBLE:
                value = defaultInput.getDouble(col);
                break;
            case BOOLEAN:
                value = defaultInput.getBoolean(col);
                break;
            case DATETIME:
                value = new Date((defaultInput.getDateTime(col)).toInstant().toEpochMilli());
                break;
            }
        }
        return value;
    }

}