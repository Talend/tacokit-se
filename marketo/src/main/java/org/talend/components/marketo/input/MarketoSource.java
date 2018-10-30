// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FILTER_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MORE_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.slf4j.Logger;
import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.CompoundKey;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.Toolbox;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "MarketoInput")
@Documentation("Marketo Input Component")
public abstract class MarketoSource extends MarketoSourceOrProcessor {

    protected final MarketoInputDataSet dataSet;

    protected Schema schema;

    protected Iterator<JsonValue> resultIterator;

    private transient static final Logger LOG = getLogger(MarketoSource.class);

    public MarketoSource(@Option("configuration") final MarketoInputDataSet dataSet, //
            final MarketoService service, //
            final Toolbox tools) {
        super(dataSet, service, tools);
        this.dataSet = dataSet;
    }

    @PostConstruct
    public void init() {
        super.init();
        schema = marketoService.getEntitySchema(dataSet);
        LOG.warn("[init] dataSet {}. Master entity schema: {}.", dataSet, schema);
        // TODO in some cases we need to fill fields property
        processBatch();
    }

    /*
     * Flow management
     */


    public JsonObject nextJson() {
        JsonValue next = null;
        boolean hasNext = resultIterator.hasNext();
        if (hasNext) {
            next = resultIterator.next();
        } else if (nextPageToken != null) {
            processBatch();
            next = resultIterator.hasNext() ? resultIterator.next() : null;
        }
        return next == null ? null : next.asJsonObject();
    }
    @Producer
    public Record next() {
        JsonValue next = null;
        boolean hasNext = resultIterator.hasNext();
        if (hasNext) {
            next = resultIterator.next();
        } else if (nextPageToken != null) {
            processBatch();
            next = resultIterator.hasNext() ? resultIterator.next() : null;
        }
        return next == null ? null : convertToRecord(next.asJsonObject(), schema);
    }

    public void processBatch() {
        JsonObject result = runAction();
        nextPageToken = result.getString(ATTR_NEXT_PAGE_TOKEN, null);
        JsonArray requestResult = result.getJsonArray(ATTR_RESULT);
        Boolean hasMore = result.getBoolean(ATTR_MORE_RESULT, true);
        if (!hasMore && requestResult != null) {
            resultIterator = requestResult.iterator();
            nextPageToken = null;
            return;
        }
        while (nextPageToken != null && requestResult == null && hasMore) {
            LOG.warn("[processBatch] looping for valid results. {}", nextPageToken);
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

    protected JsonObject generateCompoundKeyPayload(String filterType, String fields) {
        Map<String, Object> ck = new HashMap<>();
        for (CompoundKey p : dataSet.getCompoundKey()) {
            ck.put(p.getKey(), p.getValue());
        }
        JsonArray input = jsonFactory.createArrayBuilder().add(jsonFactory.createObjectBuilder(ck).build()).build();
        JsonArray jfields = jsonFactory.createArrayBuilder(Arrays.asList(fields.split(","))).build();
        JsonObject payload = jsonFactory.createObjectBuilder()//
                .add(ATTR_FILTER_TYPE, filterType) //
                .add(ATTR_FIELDS, jfields) //
                .add(ATTR_INPUT, input) //
                .build();
        LOG.warn("[generateCompoundKeyPayload] payload: {}.", payload);
        return payload;
    }

}
