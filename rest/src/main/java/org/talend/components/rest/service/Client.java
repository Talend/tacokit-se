package org.talend.components.rest.service;

import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
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

import javax.json.JsonObject;
import java.util.Map;

public interface Client extends HttpClient {

    @Request
    @UseConfigurer(RequestConfigurer.class)
    Response<JsonObject> execute(@ConfigurerOption("configuration") RequestConfig config,
            @ConfigurerOption("httpClient") Client httpClient, // Needed to do intermediate call for example to get oauth token
            @HttpMethod String httpMethod, @Url String url, @Headers Map<String, String> headers,
            @QueryParams Map<String, String> queryParams, RequestBody body);

}
