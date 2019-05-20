// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.output;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;

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

@Slf4j
@Version
@Processor(family = "Marketo", name = "Output")
@Icon(value = IconType.MARKETO)
@Documentation("Marketo Output Component")
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
        log.debug("[begin] clearing records.");
        records.clear();
    }

    @ElementListener
    public void map(@Input final Record incomingData) {
        JsonObject data = marketoService.toJson(incomingData);
        log.debug("[map] received: {}.", data);
        records.add(data);
    }

    @AfterGroup
    public void flush() {
        log.warn("[flush] called. Processing {} records.", records.size());
        if (records.isEmpty()) {
            return;
        }
        if (records.size() > 300) {
            log.error("[flush] Max batch size is set above API max batch size (300): {}.", records.size());
        }
        JsonObject payload = strategy.getPayload(records);
        log.debug("[map] payload : {}.", payload);
        JsonObject result = strategy.runAction(payload);
        log.debug("[map] result  : {}.", result);
        result.getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class).stream().filter(strategy::isRejected).forEach(e -> {
            log.error(getErrors(e.getJsonArray(ATTR_REASONS)));
        });
    }

}
