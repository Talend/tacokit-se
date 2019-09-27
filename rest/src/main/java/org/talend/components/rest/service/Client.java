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
package org.talend.components.rest.service;

import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.service.http.ConfigurerOption;
import org.talend.sdk.component.api.service.http.Headers;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.HttpMethod;
import org.talend.sdk.component.api.service.http.QueryParams;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.Url;
import org.talend.sdk.component.api.service.http.UseConfigurer;

import javax.json.JsonObject;
import java.util.Map;

public interface Client extends HttpClient{

    @Request
    @UseConfigurer(SimpleAuthConfigurer.class)
    Response<JsonObject> execute(@ConfigurerOption("configuration") RequestConfig config,
            @ConfigurerOption("httpClient") Client httpClient, // Needed to do intermediate call for example to get oauth token
            @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
            @QueryParams() Map<String, String> queryParams, RequestBody body);

    @Request
    @UseConfigurer(DigestAuthConfigurer.class)
    Response<JsonObject> executeWithDigestAuth(@ConfigurerOption("configuration") RequestConfig config,
                                 @ConfigurerOption("httpClient") Client httpClient, // Needed to do intermediate call for example to get oauth token
                                 @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
                                 @QueryParams() Map<String, String> queryParams, RequestBody body);

}
