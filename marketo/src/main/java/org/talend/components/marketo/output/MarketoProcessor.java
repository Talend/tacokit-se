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
package org.talend.components.marketo.output;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;

import org.talend.components.marketo.MarketoRuntimeException;
import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.REST_API_LIMIT;

@Slf4j
@Version
@Processor(family = "Marketo", name = "Output")
@Icon(value = IconType.MARKETO)
@Documentation("Marketo output component")
public class MarketoProcessor extends MarketoSourceOrProcessor {

    protected final MarketoOutputConfiguration configuration;

    private ProcessorStrategy strategy;

    private List<JsonObject> records;

    public MarketoProcessor(@Option("configuration") final MarketoOutputConfiguration configuration, //
            final MarketoService service) {
        super(configuration.getDataSet(), service);
        this.configuration = configuration;
        records = new ArrayList<>();
        strategy = new LeadStrategy(configuration, service);
    }

    @PostConstruct
    @Override
    public void init() {
        strategy.init();
    }

    @BeforeGroup
    public void begin() {
        records.clear();
    }

    @ElementListener
    public void map(@Input final Record incomingData) {
        JsonObject data = marketoService.toJson(incomingData);
        records.add(data);
    }

    @AfterGroup
    public void flush() {
        log.info("[flush] called. Processing {} records.", records.size());
        if (records.isEmpty()) {
            return;
        }
        if (records.size() > REST_API_LIMIT) {
            String msg = String.format("[flush] Max batch size is set above API limit (%d): %d.", REST_API_LIMIT, records.size());
            log.error(msg);
            throw new MarketoRuntimeException(msg);
        }
        JsonObject payload = strategy.getPayload(records);
        JsonObject result = strategy.runAction(payload);
        result.getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class).stream().filter(strategy::isRejected).forEach(e -> {
            log.error(getErrors(e.getJsonArray(ATTR_REASONS)));
        });
    }

}
