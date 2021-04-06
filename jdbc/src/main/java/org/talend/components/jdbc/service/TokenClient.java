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

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Configurer;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.UseConfigurer;

public interface TokenClient extends HttpClient {

    String CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";

    String CONTENT_TYPE = "Content-Type";

    String CONTENT_LENGTH = "Content-Length";

    @UseConfigurer(TokenConfigurer.class)
    @Request(method = "POST")
    Response<JsonObject> getAccessToken(String payload);

    class TokenConfigurer implements Configurer {

        @Override
        public void configure(Connection connection, ConfigurerConfiguration configuration) {
            connection.withHeader(CONTENT_TYPE, CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED).withHeader(CONTENT_LENGTH,
                    String.valueOf(connection.getPayload().length));
        }

    }
}
