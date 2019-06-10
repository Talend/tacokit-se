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
package org.talend.components.marketo;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.talend.components.marketo.dataset.MarketoDataSet;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.http.Response;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_CODE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ERRORS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MESSAGE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SUCCESS;

@Slf4j
public class MarketoSourceOrProcessor implements Serializable {

    protected final MarketoService marketoService;

    protected final I18nMessage i18n;

    protected final JsonBuilderFactory jsonFactory;

    protected final JsonReaderFactory jsonReader;

    protected final JsonWriterFactory jsonWriter;

    protected transient String nextPageToken;

    protected transient String accessToken;

    private MarketoDataSet dataSet;

    public MarketoSourceOrProcessor(@Option("configuration") final MarketoDataSet dataSet, //
            final MarketoService service) {
        this.dataSet = dataSet;
        this.i18n = service.getI18n();
        this.jsonFactory = service.getJsonFactory();
        this.jsonReader = service.getJsonReader();
        this.jsonWriter = service.getJsonWriter();
        this.marketoService = service;
    }

    @PostConstruct
    public void init() {
        nextPageToken = null;
        accessToken = marketoService.retrieveAccessToken(dataSet);
    }

    /**
     * Convert Marketo Errors array to a single String (generally for Exception throwing).
     *
     * @param errors
     * @return flattened string
     */
    public String getErrors(JsonArray errors) {
        StringBuffer error = new StringBuffer();
        for (JsonObject json : errors.getValuesAs(JsonObject.class)) {
            error.append(String.format("[%s] %s", json.getString(ATTR_CODE), json.getString(ATTR_MESSAGE)));
        }

        return error.toString();
    }

    /**
     * Handle a typical Marketo response's payload to API call.
     *
     * @param response the http response
     * @return Marketo API result
     */
    public JsonObject handleResponse(final Response<JsonObject> response) {
        log.debug("[handleResponse] [{}] body: {}.", response.status(), response.body());
        if (response.status() == MarketoApiConstants.HTTP_STATUS_OK) {
            if (response.body().getBoolean(ATTR_SUCCESS)) {
                return response.body();
            } else {
                throw new MarketoRuntimeException(getErrors(response.body().getJsonArray(ATTR_ERRORS)));
            }
        }
        throw new MarketoRuntimeException(response.error(String.class));
    }

}
