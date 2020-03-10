/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.service;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.UseConfigurer;

public interface AccessTokenProvider extends HttpClient {
    // https://login.microsoftonline.com/<tenantid>/oauth2/v2.0/token

    /**
     * Headers : "Content-Type: application/x-www-form-urlencoded"
     * Body :
     * {"client_id": <CLIENT_ID>,
     * "client_secret": <CLIENT_SECRET>,
     * "scope" : "https://storage.azure.com/.default",
     * "grant_type" : "client_credentials"
     * }
     */
    @UseConfigurer(AccessTokenConfigurer.class)
    @Request(path = "/{tenantId}/oauth2/v2.0/token", method = "POST")
    Response<JsonObject> getAccessToken( //
            @Path("tenantId") String tenantId, //
            String payload //
    );

}
