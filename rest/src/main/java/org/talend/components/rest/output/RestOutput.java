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
package org.talend.components.rest.output;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import java.io.Serializable;

@Slf4j
@Getter
@Version
@Processor(name = "Output")
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-rest")
@Documentation("Http REST Output component")
public class RestOutput implements Serializable {

    private final RequestConfig config;

    private final RestService client;

    public RestOutput(@Option("configuration") final RequestConfig config, final RestService client) {
        this.config = config;
        this.client = client;
    }

    @PostConstruct
    public void init() {
        // no-op
    }

    @ElementListener
    public void process(final Record input) {
        client.execute(config, input);
    }

}
