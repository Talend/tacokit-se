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
package org.talend.components.marketo;

import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CODE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ERRORS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MESSAGE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SUCCESS;
import static org.talend.components.marketo.service.AuthorizationClient.CLIENT_CREDENTIALS;

import java.io.Serializable;
import java.io.StringReader;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.runtime.beam.transform.avro.JsonIndexedRecord;

public class MarketoSourceOrProcessor implements Serializable {

    protected final I18nMessage i18n;

    protected final AuthorizationClient authorizationClient;

    protected final JsonBuilderFactory jsonFactory;

    protected final JsonReaderFactory jsonReader;

    protected final JsonWriterFactory jsonWriter;

    protected transient String nextPageToken;

    protected transient String accessToken;

    private MarketoDataSet dataSet;

    private transient static final Logger LOG = getLogger(MarketoSourceOrProcessor.class);

    public MarketoSourceOrProcessor(@Option("configuration") final MarketoDataSet dataSet, final I18nMessage i18n,
            final JsonBuilderFactory jsonFactory, //
            final JsonReaderFactory jsonReader, //
            final JsonWriterFactory jsonWriter, //
            //
            final AuthorizationClient authorizationClient) {
        this.dataSet = dataSet;
        this.i18n = i18n;
        this.jsonFactory = jsonFactory;
        this.jsonReader = jsonReader;
        this.jsonWriter = jsonWriter;
        this.authorizationClient = authorizationClient;
        this.authorizationClient.base(this.dataSet.getDataStore().getEndpoint());
    }

    @PostConstruct
    public void init() {
        nextPageToken = null;
        retrieveAccessToken();
    }

    public String getAccessToken() {
        if (accessToken == null) {
            retrieveAccessToken();
        }
        return accessToken;
    }

    /**
     * Retrieve an set an access token for using API
     */
    public void retrieveAccessToken() {
        Response<JsonObject> result = authorizationClient.getAuthorizationToken(CLIENT_CREDENTIALS,
                dataSet.getDataStore().getClientId(), dataSet.getDataStore().getClientSecret());
        LOG.debug("[retrieveAccessToken] [{}] : {}.", result.status(), result.body());
        if (result.status() == 200) {
            accessToken = result.body().getString(ATTR_ACCESS_TOKEN);
        } else {
            String error = i18n.accessTokenRetrievalError(result.status(), result.headers().toString());
            LOG.error("[retrieveAccessToken] {}", error);
            throw new RuntimeException(error);
        }
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
        LOG.warn("[handleResponse] [{}] body: {}.", response.status(), response.body());
        if (response.status() == 200) {
            if (response.body().getBoolean(ATTR_SUCCESS)) {
                return response.body();
            } else {
                throw new RuntimeException(getErrors(response.body().getJsonArray(ATTR_ERRORS)));
            }
        }
        throw new RuntimeException(response.error(String.class));
    }

    public JsonObject toJson(final IndexedRecord record) {
        JsonReader reader = jsonReader.createReader(new StringReader(record.toString()));
        Throwable throwable = null;
        JsonObject json;
        try {
            json = reader.readObject();
        } catch (Throwable throwable1) {
            throwable = throwable1;
            throw throwable1;
        } finally {
            if (reader != null) {
                if (throwable != null) {
                    try {
                        reader.close();
                    } catch (Throwable throwable2) {
                        throwable.addSuppressed(throwable2);
                    }
                } else {
                    reader.close();
                }
            }
        }
        return json;
    }

    public IndexedRecord toIndexedRecord(final JsonObject json, final Schema schema) {
        return new JsonIndexedRecord(json, schema);
    }
}
