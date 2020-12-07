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
package org.talend.components.rest.source;

import java.io.Serializable;
import java.util.Iterator;

import javax.annotation.PostConstruct;

import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RecordBuilderService;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-rest")
@Emitter(name = "Input")
@Documentation("Http REST Input component")
@Pollable(name = "Polling", resumeMethod = "resume")
public class RestEmitter implements Serializable {

    private final RequestConfig config;

    private final RestService client;

    private final RecordBuilderService recordBuilder;

    private Iterator<Record> items;

    private boolean done;

    public void resume(Object configuration) {
        done = false;
    }

    public RestEmitter(@Option("configuration") final RequestConfig config, final RestService client,
            final RecordBuilderService recordBuilder) {
        this.config = config;
        this.client = client;
        this.recordBuilder = recordBuilder;
    }

    @PostConstruct
    public void init() {
        client.checkBaseURL(config.getDataset().getDatastore().getBase());
    }

    @Producer
    public Record next() {
        if (done && items == null) {
            return null;
        }

        if (items == null && !done) {
            done = true;
            items = recordBuilder.buildFixedRecord(client.execute(config), config);
        }

        final Record r = items.hasNext() ? items.next() : null;

        if (!items.hasNext()) {
            items = null;
        }

        return r;

    }

}
