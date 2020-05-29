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

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;

import static org.talend.components.adlsgen2.service.AdlsGen2Service.handleResponse;

@Service
public class AdlsActiveDirectoryService {

    @Service
    private AccessTokenProvider tokenGetter;

    public String getActiveDirAuthToken(AdlsGen2Connection connection) {
        tokenGetter.base("https://login.microsoftonline.com/");
        String requestBodyFormat = "grant_type=client_credentials&scope=https://storage.azure.com/.default&client_id=%s&client_secret=%s";

        Response<JsonObject> result = handleResponse(tokenGetter.getAccessToken(connection.getTenantId(),
                String.format(requestBodyFormat, connection.getClientId(), connection.getClientSecret())));

        return result.body().getString("access_token");
    }

}
