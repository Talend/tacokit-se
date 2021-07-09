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
package org.talend.components.zendesk.source;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;

import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.zendesk.helpers.CommonHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Documentation("TODO fill the documentation for this source")
public class ZendeskInputSource implements Serializable {

    private final ZendeskInputMapperConfiguration configuration;

    private ZendeskHttpClientService zendeskHttpClientService;

    private InputIterator itemIterator;

    private Messages i18n;

    private final RecordBuilderFactory builderFactory;

    final JsonToRecord jsonToRecord;

    public ZendeskInputSource(@Option("configuration") final ZendeskInputMapperConfiguration configuration,
            final ZendeskHttpClientService zendeskHttpClientService, final RecordBuilderFactory builderFactory,
            final Messages i18n) {
        this.configuration = configuration;
        this.zendeskHttpClientService = zendeskHttpClientService;
        this.builderFactory = builderFactory;
        this.i18n = i18n;
        this.jsonToRecord = new JsonToRecord(this.builderFactory, false);
    }

    @PostConstruct
    public void init() {
        itemIterator = CommonHelper.getInputIterator(zendeskHttpClientService, configuration, i18n);
    }

    @Producer
    public Record next() {
        JsonObject next = itemIterator.next();
        if (next != null) {
            return jsonToRecord.toRecord(next);
        } else {
            return null;
        }
    }
}