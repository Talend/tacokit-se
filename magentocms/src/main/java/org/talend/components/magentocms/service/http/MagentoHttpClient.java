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
package org.talend.components.magentocms.service.http;

import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.http.ConfigurerOption;
import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.QueryParams;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.UseConfigurer;
import org.talend.sdk.component.api.service.http.configurer.oauth1.OAuth1;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.Map;

public interface MagentoHttpClient extends HttpClient {

    String HEADER_Authorization = "Authorization";

    String HEADER_Content_Type = "Content-Type";

    @Request(path = "{requestPath}")
    @Documentation("read record from the table according to the data set definition")
    Response<JsonObject> get(@Path("requestPath") String requestPath, @Header(HEADER_Authorization) String auth,
            @QueryParams Map<String, String> qp);

    @Request(path = "{requestPath}")
    @UseConfigurer(OAuth1.Configurer.class)
    @Documentation("read record from the table according to the data set definition. It uses OAuth1 authorization")
    Response<JsonObject> get(@Path("requestPath") String requestPath,
            @ConfigurerOption("option") OAuth1.Configuration oauthOptions, @QueryParams Map<String, String> qp);

    default Response<JsonObject> getRecords(String requestPath, String auth, Map<String, String> queryParameters) {
        return get(requestPath, auth, queryParameters);
    }

    default Response<JsonObject> getRecords(String requestPath, OAuth1.Configuration oaut1Config,
            Map<String, String> queryParameters) {
        return get(requestPath, oaut1Config, queryParameters);
    }

    @Request(method = "POST", path = "{requestPath}")
    @Documentation("read record from the table according to the data set definition")
    Response<JsonObject> post(@Path("requestPath") String requestPath, @Header(HEADER_Authorization) String auth,
            @Header(HEADER_Content_Type) String contentType, JsonObject record);

    @Request(method = "POST", path = "{requestPath}")
    @UseConfigurer(OAuth1.Configurer.class)
    @Documentation("read record from the table according to the data set definition. It uses OAuth1 authorization")
    Response<JsonObject> post(@Path("requestPath") String requestPath,
            @ConfigurerOption("option") OAuth1.Configuration oauthOptions, @Header(HEADER_Content_Type) String contentType,
            JsonObject record);

    default Response<JsonObject> postRecords(String requestPath, String auth, JsonObject dataList) {
        return post(requestPath, auth, "application/json", dataList);
    }

    default Response<JsonObject> postRecords(String requestPath, OAuth1.Configuration oaut1Config, JsonObject dataList) {
        return post(requestPath, oaut1Config, "application/json", dataList);
    }

    @Request(method = "POST", path = "{requestPath}")
    @Documentation("read record from the table according to the data set definition. It uses OAuth1 authorization")
    Response<JsonValue> getToken(@Path("requestPath") String requestPath, @Header(HEADER_Content_Type) String contentType,
            JsonObject body);

    default Response<JsonValue> getToken(String requestPath, JsonObject body) {
        return getToken(requestPath, "application/json", body);
    }

}
