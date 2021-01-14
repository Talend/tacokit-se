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

    protected String getErrors(JsonArray errors) {
        return marketoService.getErrors(errors);
    }

    protected JsonObject handleResponse(Response<JsonObject> response) {
        return marketoService.handleResponse(response);
    }

}
