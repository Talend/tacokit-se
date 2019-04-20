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

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.List;

import org.talend.components.mongodb.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import org.talend.components.mongodb.service.MongoDBService;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "MongoDBInput")
@PartitionMapper(name = "MongoDBInput")
@Documentation("Input mapper for MongoDB")
public class MongoDBInputMapper implements Serializable {

    private final MongoDBInputMapperConfiguration configuration;

    private final MongoDBService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private final I18nMessage i18nMessage;

    public MongoDBInputMapper(@Option("configuration") final MongoDBInputMapperConfiguration configuration,
            final MongoDBService service, final RecordBuilderFactory recordBuilderFactory, final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18nMessage = i18nMessage;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<MongoDBInputMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter
    public MongoDBInputSource createWorker() {
        return new MongoDBInputSource(configuration, service, recordBuilderFactory, i18nMessage);
    }
}