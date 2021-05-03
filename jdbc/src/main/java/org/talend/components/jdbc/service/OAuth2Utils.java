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

import static java.util.Optional.of;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.json.JsonObject;
import org.talend.components.jdbc.datastore.GrantType;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.http.Response;

public final class OAuth2Utils {

    static final String ACCESS_TOKEN_NAME = "access_token";

    static final String ERROR_DESCRIPTION = "error_description";

    static final String ERROR_SUMMARY = "errorSummary";

    static final String USERNAME = "username";

    static final String PASSWORD = "password";

    static final String GRANT_TYPE = "grant_type";

    static final String SCOPE = "scope";

    static final char PARAMETER_SEPARATOR = '&';

    static final char VALUE_SEPARATOR = '=';

    static final String AUTHORIZATION_PREFIX = "Basic ";

    private OAuth2Utils() {
    }

    static String getAccessToken(JdbcConnection connection, TokenClient tokentClient, I18nMessage i18n) {
        Response<JsonObject> response = tokentClient.getAccessToken(connection.getOauthTokenEndpoint(),
                getAuthorization(connection), getPayload(connection));
        checkResponse(response, i18n);

        return response.body().getString(ACCESS_TOKEN_NAME);
    }

    private static void checkResponse(Response<JsonObject> response, I18nMessage i18n) {
        of(response).filter(r -> r.status() != 200).map(r -> r.error(JsonObject.class)).map(OAuth2Utils::getErrorDescription)
                .map(i18n::errorAccessTokenResponse).ifPresent(OAuth2Utils::throwComponentException);
    }

    private static String getErrorDescription(JsonObject errorResponse) {
        return errorResponse.containsKey(ERROR_DESCRIPTION) ? errorResponse.getString(ERROR_DESCRIPTION)
                : errorResponse.containsKey(ERROR_SUMMARY) ? errorResponse.getString(ERROR_SUMMARY) : "";
    }

    private static void throwComponentException(String errorDescription) {
        throw new ComponentException(errorDescription);
    }

    private static String getPayload(JdbcConnection connection) {
        StringBuilder builder = new StringBuilder().append(GRANT_TYPE).append(VALUE_SEPARATOR)
                .append(connection.getGrantType().name().toLowerCase()).append(PARAMETER_SEPARATOR).append(SCOPE)
                .append(VALUE_SEPARATOR).append(connection.getScope());

        if (connection.getGrantType() == GrantType.PASSWORD) {
            builder.append(PARAMETER_SEPARATOR).append(USERNAME).append(VALUE_SEPARATOR).append(connection.getOauthUsername())
                    .append(PARAMETER_SEPARATOR).append(PASSWORD).append(VALUE_SEPARATOR).append(connection.getOauthPassword());
        }
        return builder.toString();
    }

    private static String getAuthorization(JdbcConnection connection) {
        return AUTHORIZATION_PREFIX + Base64.getEncoder()
                .encodeToString((connection.getClientId() + ':' + connection.getClientSecret()).getBytes(StandardCharsets.UTF_8));
    }
}
