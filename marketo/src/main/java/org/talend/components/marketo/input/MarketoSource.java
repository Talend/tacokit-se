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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MORE_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;
import javax.json.JsonWriterFactory;

import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "MarketoInput")
@Documentation("Marketo Input Component")
public abstract class MarketoSource extends MarketoSourceOrProcessor {

    protected final MarketoInputDataSet dataSet;

    protected Iterator<JsonValue> resultIterator;

    private transient static final Logger LOG = getLogger(MarketoSource.class);

    public MarketoSource(@Option("configuration") final MarketoInputDataSet dataSet, //
            final I18nMessage i18n, //
            final JsonBuilderFactory jsonFactory, //
            final JsonReaderFactory jsonReader, //
            final JsonWriterFactory jsonWriter, //
            final AuthorizationClient authorizationClient) {
        super(dataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient);
        this.dataSet = dataSet;
    }

    @PostConstruct
    public void init() {
        super.init();
        processBatch();
    }

    /*
     * Flow management
     */

    @Producer
    public JsonObject next() {
        JsonValue next = null;
        boolean hasNext = resultIterator.hasNext();
        if (hasNext) {
            next = resultIterator.next();
        } else if (nextPageToken != null) {
            processBatch();
            next = resultIterator.hasNext() ? resultIterator.next() : null;
        }
        LOG.error("[next] is {}.", next);
        return next == null ? null : next.asJsonObject();
    }

    public IndexedRecord nextIndexedRecord() {
        JsonObject nextIR = next();
        return nextIR == null ? null : toIndexedRecord(nextIR, dataSet.getAvroSchema());
    }

    public void processBatch() {
        JsonObject result = runAction();
        LOG.warn("[processBatch] {} {}", accessToken, result);
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
        // nextPageToken = result.getString(ATTR_NEXT_PAGE_TOKEN, null);
    }

    public abstract JsonObject runAction();
}
