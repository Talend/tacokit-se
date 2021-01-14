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
package org.talend.components.rest.service.client;

import org.talend.components.common.service.http.common.UserNamePassword;
import org.talend.components.common.service.http.basic.BasicAuthConfigurer;
import org.talend.components.common.service.http.bearer.BearerAuthConfigurer;
import org.talend.components.common.service.http.digest.DigestAuthConfigurer;
import org.talend.components.common.service.http.digest.DigestAuthContext;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.I18n;
import org.talend.sdk.component.api.service.http.Codec;
import org.talend.sdk.component.api.service.http.ConfigurerOption;
import org.talend.sdk.component.api.service.http.Headers;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.HttpMethod;
import org.talend.sdk.component.api.service.http.QueryParams;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.Url;
import org.talend.sdk.component.api.service.http.UseConfigurer;

import java.io.InputStream;
import java.util.Map;

public interface Client extends HttpClient {

    @Request
    @UseConfigurer(RestConfigurer.class)
    @Codec(encoder = { RequestEncoder.class })
    Response<InputStream> execute(@ConfigurerOption("i18n") I18n i18n, @ConfigurerOption("configuration") RequestConfig config,
            @ConfigurerOption("httpClient") Client httpClient, // Needed to do intermediate call for example to get oauth token
            @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
            @QueryParams(/* default encode = true */) Map<String, String> queryParams, Body body);

    @Request
    @UseConfigurer(RestConfigurer.class)
    @Codec(encoder = { RequestEncoder.class })
    Response<InputStream> executeWithBasicAuth(@ConfigurerOption("i18n") I18n i18n,
            @ConfigurerOption(BasicAuthConfigurer.BASIC_CONTEXT_CONF) UserNamePassword context,
            @ConfigurerOption("configuration") RequestConfig config, @ConfigurerOption("httpClient") Client httpClient,
            @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
            @QueryParams(/* default encode = true */) Map<String, String> queryParams, Body body);

    @Request
    @UseConfigurer(RestConfigurer.class)
    @Codec(encoder = { RequestEncoder.class })
    Response<InputStream> executeWithBearerAuth(@ConfigurerOption("i18n") I18n i18n,
            @ConfigurerOption(BearerAuthConfigurer.BEARER_TOKEN_CONF) String token,
            @ConfigurerOption("configuration") RequestConfig config, @ConfigurerOption("httpClient") Client httpClient,
            @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
            @QueryParams(/* default encode = true */) Map<String, String> queryParams, Body body);

    @Request
    @UseConfigurer(RestConfigurer.class)
    @Codec(encoder = { RequestEncoder.class })
    Response<InputStream> executeWithDigestAuth(@ConfigurerOption("i18n") I18n i18n,
            @ConfigurerOption(DigestAuthConfigurer.DIGEST_CONTEXT_CONF) DigestAuthContext context,
            @ConfigurerOption("configuration") RequestConfig config, @ConfigurerOption("httpClient") Client httpClient,
            @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
            @QueryParams(/* default encode = true */) Map<String, String> queryParams, Body body);

}
