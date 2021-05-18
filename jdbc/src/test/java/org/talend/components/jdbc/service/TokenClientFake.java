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
package org.talend.components.jdbc.service;

import static org.talend.components.jdbc.service.OAuth2Utils.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Response;

public class TokenClientFake implements TokenClient {

    static final String CORRECT_URL = "https://talend.com";

    static final String CORRECT_PARAMETER = "correct";

    static final String ACCESS_TOKEN = "acces_token";

    @Override
    public void base(String base) {
        // This one is a deprecated.
    }

    @Override
    public Response<JsonObject> getAccessToken(String base, String authorization, String payload) {
        if (!CORRECT_URL.equalsIgnoreCase(base)) {
            return new ResponseImpl(404, null,
                    createJsonObject(ERROR_SUMMARY, "The endpoint does not support the provided HTTP method."));
        }
        String encodedAuth = authorization.substring(authorization.indexOf(" ") + 1);
        String[] authDetails = new String(Base64.getDecoder().decode(encodedAuth), StandardCharsets.UTF_8).split(":");
        if (!CORRECT_PARAMETER.equals(authDetails[0])) {
            return new ResponseImpl(401, null, createJsonObject(ERROR_SUMMARY, "Invalid value for 'client_id' parameter."));
        }
        if (!CORRECT_PARAMETER.equals(authDetails[1])) {
            return new ResponseImpl(401, null,
                    createJsonObject(ERROR_DESCRIPTION, "The client secret supplied for a confidential client is invalid."));
        }

        if (payload.contains("grant_type=password")
                && !(payload.contains("username=" + CORRECT_PARAMETER) && payload.contains("password=" + CORRECT_PARAMETER))) {
            return new ResponseImpl(401, null, createJsonObject(ERROR_DESCRIPTION, "The credentials provided were invalid."));
        }

        if (!payload.contains("scope=" + CORRECT_PARAMETER)) {
            return new ResponseImpl(401, null, createJsonObject(ERROR_DESCRIPTION,
                    "One or more scopes are not configured for the authorization server resource."));
        }
        return new ResponseImpl(200, createJsonObject(ACCESS_TOKEN_NAME, ACCESS_TOKEN), null);
    }

    private JsonObject createJsonObject(String name, String message) {
        return Json.createObjectBuilder().add(name, message).build();
    }

    private class ResponseImpl implements Response<JsonObject> {

        final int status;

        final JsonObject body;

        final JsonObject error;

        private ResponseImpl(int status, JsonObject body, JsonObject error) {
            this.status = status;
            this.body = body;
            this.error = error;
        }

        @Override
        public int status() {
            return status;
        }

        @Override
        public Map<String, List<String>> headers() {
            return null;
        }

        @Override
        public JsonObject body() {
            return body;
        }

        @SuppressWarnings("hiding")
        @Override
        public <JsonObject> JsonObject error(Class<JsonObject> type) {
            return type.cast(error);
        }

    }
}
