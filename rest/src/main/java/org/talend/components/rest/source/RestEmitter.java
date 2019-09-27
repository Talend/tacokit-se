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
package org.talend.components.rest.source;

import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-rest")
@Emitter(name = "Input")
@Documentation("Http REST Input component")
public class RestEmitter implements Serializable {

    private final RequestConfig config;

    private final RestService client;

    private final Queue<Record> records = new LinkedList<>();

    public RestEmitter(@Option("configuration") final RequestConfig config, final RestService client) {
        this.config = config;
        this.client = client;
    }

    @PostConstruct
    public void init() {
        Record record = client.execute(config);
        records.add(record);
    }

    @Producer
    public Record next() {
        return records.poll();
    }

}
