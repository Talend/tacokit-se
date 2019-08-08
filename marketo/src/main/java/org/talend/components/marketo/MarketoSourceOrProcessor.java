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
