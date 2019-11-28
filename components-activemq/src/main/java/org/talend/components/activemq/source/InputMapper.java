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
package org.talend.components.activemq.source;

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.List;

import javax.json.JsonBuilderFactory;

import org.talend.components.activemq.service.ActiveMQService;
import org.talend.components.activemq.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;

import org.talend.sdk.component.api.service.Service;

@Version(1)
@Icon(value = Icon.IconType.ACTIVEMQ)
@PartitionMapper(name = "Input")
@Documentation("Purpose of this class is to create an actual worker")
public class InputMapper implements Serializable {

    private final InputMapperConfiguration configuration;

    private final ActiveMQService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    @Service
    private final I18nMessage i18nMessage;

    public InputMapper(@Option("configuration") final InputMapperConfiguration configuration, final ActiveMQService service,
            final JsonBuilderFactory jsonBuilderFactory, final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.i18nMessage = i18nMessage;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<InputMapper> split(@PartitionSize final long bundles) {
        // overall idea here is to split the work related to configuration1 in bundles of size "bundles"
        //
        // for instance if your estimateSize() returned 1000 and you can run on 10 nodes
        // then the environment can decide to run it concurrently (10 * 100).
        // In this case bundles = 100 and we must try to return 10 InputMapper with 1/10 of the overall work each.
        //
        // default implementation returns this which means it doesn't support the work to be split
        return singletonList(this);
    }

    @Emitter
    public InputSource createWorker() {
        // here we create an actual worker,
        // you are free to rework the configuration1 etc but our default generated implementation
        // propagates the partition mapper entries.
        return new InputSource(configuration, service, jsonBuilderFactory, i18nMessage);
    }
}