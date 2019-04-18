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
package org.talend.components.marketo.input;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoInputConfiguration;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_MORE_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "MarketoInput")
@Documentation("Marketo Input Component")
public abstract class MarketoSource extends MarketoSourceOrProcessor {

    protected final MarketoInputConfiguration configuration;

    protected Map<String, Schema.Entry> schema;

    protected Iterator<JsonValue> resultIterator;

    public MarketoSource(@Option("configuration") final MarketoInputConfiguration configuration, //
            final MarketoService service) {
        super(configuration.getDataSet(), service);
        this.configuration = configuration;
    }

    private Map<String, Entry> buildSchemaMap(final Schema entitySchema) {
        log.debug("[buildSchemaMap] {}", entitySchema);
        Map<String, Entry> s = new HashMap<>();
        if (entitySchema != null) {
            for (Entry entry : entitySchema.getEntries()) {
                s.put(entry.getName(), entry);
            }
        }
        return s;
    }

    @PostConstruct
    public void init() {
        super.init();
        schema = buildSchemaMap(marketoService.getEntitySchema(configuration));
        log.debug("[init] configuration {}. Master entity schema: {}.", configuration, schema);
        processBatch();
    }
    /*
     * Flow management
     */

    @Producer
    public Record next() {
        JsonValue next = null;
        if (resultIterator == null) {
            return null;
        }
        boolean hasNext = resultIterator.hasNext();
        if (hasNext) {
            next = resultIterator.next();
        } else if (nextPageToken != null) {
            processBatch();
            next = resultIterator.hasNext() ? resultIterator.next() : null;
        }
        return next == null ? null : marketoService.convertToRecord(next.asJsonObject(), schema);
    }

    public void processBatch() {
        JsonObject result = runAction();
        nextPageToken = result.getString(ATTR_NEXT_PAGE_TOKEN, null);
        log.warn("[processBatch] result {}", result);
        JsonArray requestResult = result.getJsonArray(ATTR_RESULT);
        Boolean hasMore = result.getBoolean(ATTR_MORE_RESULT, true);
        if (!hasMore && requestResult != null) {
            resultIterator = requestResult.iterator();
            nextPageToken = null;
            return;
        }
        while (nextPageToken != null && requestResult == null && hasMore) {
            log.debug("[processBatch] looping for valid results. {}", nextPageToken);
            result = runAction();
            nextPageToken = result.getString(ATTR_NEXT_PAGE_TOKEN, null);
            requestResult = result.getJsonArray(ATTR_RESULT);
            hasMore = result.getBoolean(ATTR_MORE_RESULT, true);
        }
        if (requestResult != null) {
            resultIterator = requestResult.iterator();
        }
    }

    public abstract JsonObject runAction();

}
