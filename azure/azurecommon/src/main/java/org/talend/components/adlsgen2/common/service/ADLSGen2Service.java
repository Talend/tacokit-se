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
package org.talend.components.adlsgen2.common.service;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.talend.components.adlsgen2.common.connection.AdlsGen2Connection;
import org.talend.components.adlsgen2.common.connection.Constants;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.http.Response;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.nanoTime;
import static org.talend.components.adlsgen2.common.connection.Constants.HeaderConstants.*;

@Slf4j
@Service
public class ADLSGen2Service implements Serializable {

    public static final String AZURE_BLOB_CONNECTION_LIST = "AZURE_BLOB_CONNECTION_LIST";

    public static final String AZURE_DATALAKE_CONNECTION_LIST = "AZURE_DATALAKE_CONNECTION_LIST";

    // ADSL Gen2
    public static final String SQLDWH_WORKING_DIR = "SQLDWH-bulk-load/temp" + nanoTime();

    public static final String ACTION_SUGGESTION_PATHS = "ACTION_SUGGESTION_PATHS";

    @Service
    private AdlsGen2APIClient client;

    @Setter // for test only
    private String createTime;

    private String getGMT() {
        return Constants.RFC1123GMTDateFormatter.format(OffsetDateTime.now());
    }

    private HttpRequestBase createClient(AdlsGen2Connection connection, URI uri, String method) throws InvalidKeyException {
        final String now = getGMT();
        HttpRequestBase requestBase;
        switch (method) {
        case "GET":
            requestBase = new HttpGet();
            break;
        case "PUT":
            requestBase = new HttpPut();
            break;
        case "PATCH":
            requestBase = new HttpPatch();
            break;
        case "DELETE":
            requestBase = new HttpDelete();
            break;

        default:
            requestBase = new HttpPost();

        }

        requestBase.setURI(uri);
        requestBase.setHeader(DATE, now);
        requestBase.setHeader(VERSION, TARGET_STORAGE_VERSION);
        requestBase.setHeader(CONTENT_TYPE, DFS_CONTENT_TYPE);
        SharedKeyUtils util = new SharedKeyUtils(connection.getAccountName(), connection.getSharedKey());
        String auth = util.buildAuthenticationSignature(requestBase);
        requestBase.setHeader(AUTHORIZATION, auth);
        return requestBase;

    }

    public SuggestionValues filesystemList(@Option("accountName") String accountName,
            @Option("accountKey") final String accountKey) {
        AdlsGen2Connection connection = new AdlsGen2Connection();
        connection.setSharedKey(accountKey);
        connection.setAccountName(accountName);

        client.base(connection.apiUrl());
        URI uri = null;
        try {
            uri = new URI(connection.apiUrl() + "?resource=account");
        } catch (URISyntaxException e) {
            return new SuggestionValues();
        }
        HttpRequestBase get = null;
        try {
            get = createClient(connection, uri, "GET");
        } catch (InvalidKeyException e) {
            return new SuggestionValues();
        }
        Response<JsonObject> jsonObjectResponse = client.filesystemList(connection, get.getFirstHeader(AUTHORIZATION).getValue(),
                get.getFirstHeader(DATE).getValue(), Constants.ATTR_ACCOUNT);
        Response<JsonObject> result = handleResponse(jsonObjectResponse);
        List<String> fs = new ArrayList<>();
        for (JsonValue v : result.body().getJsonArray(Constants.ATTR_FILESYSTEMS)) {
            fs.add(v.asJsonObject().getString(Constants.ATTR_NAME));
        }
        log.info("fs: {}", fs);
        return new SuggestionValues(true, fs.stream().map(e -> new SuggestionValues.Item(e, e)).collect(Collectors.toList()));
    }

    public List<String> pathList(@Option("adlsGen2Connection") final AdlsGen2Connection connection, String filesystem,
            boolean recursive, String path, boolean isDirectory) throws URISyntaxException, InvalidKeyException {
        client.base(connection.apiUrl());
        URI uri = new URI(
                connection.apiUrl() + "/" + filesystem + "?resource=filesystem&recursive=" + recursive + "&directory=" + path);
        HttpRequestBase get = createClient(connection, uri, "GET");

        Response<JsonObject> jsonObjectResponse = client.pathList(connection, get.getFirstHeader(AUTHORIZATION).getValue(),
                get.getFirstHeader(DATE).getValue(), recursive, filesystem, path);
        if (404 == jsonObjectResponse.status()) {
            List<String> strings = jsonObjectResponse.headers().get("x-ms-error-code");
            if (strings.contains("PathNotFound")) {
                log.warn("Path: " + path + " not found");
                return Collections.emptyList();
            }
        }
        Response<JsonObject> result = handleResponse(jsonObjectResponse);
        List<String> fs = new ArrayList<>();
        if (!isDirectory) {
            for (JsonValue v : result.body().getJsonArray(Constants.ATTR_PATHS)) {
                fs.add(v.asJsonObject().getString(Constants.ATTR_NAME));
            }
        } else {
            for (JsonValue v : result.body().getJsonArray(Constants.ATTR_PATHS)) {
                if ("true".equals(v.asJsonObject().getString("isDirectory", "false")))
                    fs.add(v.asJsonObject().getString(Constants.ATTR_NAME));
            }
        }
        log.info("fs: {}", fs);
        return fs;
    }

    @Suggestions(ACTION_SUGGESTION_PATHS)
    public SuggestionValues pathList(@Option("accountName") String accountName, @Option("accountKey") final String accountKey,
            String filesystem) {
        AdlsGen2Connection connection = new AdlsGen2Connection();
        connection.setSharedKey(accountKey);
        connection.setAccountName(accountName);
        List<String> paths = Collections.emptyList();
        try {
            paths = pathList(connection, filesystem, false, SQLDWH_WORKING_DIR, true);
        } catch (URISyntaxException | InvalidKeyException e) {
            return new SuggestionValues();
        }
        return new SuggestionValues(true, paths.stream().map(e -> new SuggestionValues.Item(e, e)).collect(Collectors.toList()));
    }

    public void pathDelete(@Configuration("connection") final AdlsGen2Connection connection, String filesystem, String path) {
        client.base(connection.apiUrl());
        try {
            String uri = String.join("/", connection.apiUrl(), filesystem, path) + "?recursive=true";
            HttpRequestBase delete = createClient(connection, new URI(uri), "DELETE");
            delete.setHeader(IF_UNMODIFIED_SINCE, createTime);

            Response<JsonObject> jsonObjectResponse = client.pathDelete(connection,
                    delete.getFirstHeader(AUTHORIZATION).getValue(), delete.getFirstHeader(DATE).getValue(), filesystem, path);
            if (404 == jsonObjectResponse.status()) {
                List<String> strings = jsonObjectResponse.headers().get("x-ms-error-code");
                if (strings.contains("PathNotFound")) {
                    log.warn(jsonObjectResponse.error(String.class));
                }
            }
        } catch (URISyntaxException | InvalidKeyException e) {
            log.warn(e.getLocalizedMessage());
        }
        log.info("Delete path " + path + " success.");
    }

    public void createPath(@Configuration("connection") final AdlsGen2Connection connection, String filesystem, String filePath)
            throws InvalidKeyException, URISyntaxException {
        final String now = getGMT();
        client.base(connection.apiUrl());

        URI uri = new URI(connection.apiUrl() + "/" + filesystem + "/" + filePath + "?resource=file");
        HttpRequestBase put = createClient(connection, uri, "PUT");

        Response<JsonObject> jsonObjectResponse = client.pathCreate(connection, put.getFirstHeader(AUTHORIZATION).getValue(),
                put.getFirstHeader(DATE).getValue(), filesystem, filePath, "");
        Response<JsonObject> result = handleResponse(jsonObjectResponse);
        log.info("Create path " + filePath + "success.");

    }

    // org.talend.sdk.component.api.service.http.HttpClient is using HttpURLConnection under the hood which does not support PATCH
    // method.
    public void appendPath(@Configuration("connection") final AdlsGen2Connection connection, String filesystem, String fullPath,
            Path tempFilePath) throws InvalidKeyException, URISyntaxException, IOException {
        final String now = getGMT();
        client.base(connection.apiUrl());
        String position = "0";// position should be 0 here
        URI uri = new URI(connection.apiUrl() + "/" + filesystem + "/" + fullPath + "?action=append&position=" + position);
        HttpPatch patch = new HttpPatch(uri);
        patch.setHeader(DATE, now);
        patch.setHeader(VERSION, TARGET_STORAGE_VERSION);
        patch.setHeader(CONTENT_TYPE, DFS_CONTENT_TYPE);
        patch.setHeader("contentLength", String.valueOf(tempFilePath.toFile().length()));// this is not the stanard Content-Length
                                                                                         // since httpclient will set it
                                                                                         // automaticly
        // patch.setHeader("x-ms-blob-type", "BlockBlob");
        HttpEntity entity = new FileEntity(tempFilePath.toFile());
        patch.setEntity(entity);

        SharedKeyUtils util = new SharedKeyUtils(connection.getAccountName(), connection.getSharedKey());
        String auth = util.buildAuthenticationSignature(patch);
        patch.setHeader(AUTHORIZATION, auth);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse execute = httpclient.execute(patch);
            if (execute.getStatusLine().getStatusCode() != Constants.HTTP_RESPONSE_CODE_202_ACCEPTED) {
                throw new IllegalArgumentException(EntityUtils.toString(execute.getEntity()));
            }
        }
    }

    // org.talend.sdk.component.api.service.http.HttpClient is using HttpURLConnection under the hood which does not support PATCH
    // method.
    public void flushPath(@Configuration("connection") final AdlsGen2Connection connection, String filesystem, String fullPath,
            long fileLongth) throws InvalidKeyException, URISyntaxException, IOException {
        final String now = getGMT();
        client.base(connection.apiUrl());
        URI uri = new URI(connection.apiUrl() + "/" + filesystem + "/" + fullPath + "?action=flush&position=" + fileLongth);
        HttpPatch patch = new HttpPatch(uri);
        patch.setHeader(DATE, now);
        patch.setHeader(VERSION, TARGET_STORAGE_VERSION);
        patch.setHeader(CONTENT_TYPE, DFS_CONTENT_TYPE);
        // patch.setHeader("x-ms-blob-type", "BlockBlob");

        SharedKeyUtils util = new SharedKeyUtils(connection.getAccountName(), connection.getSharedKey());
        String auth = util.buildAuthenticationSignature(patch);
        patch.setHeader(AUTHORIZATION, auth);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse execute = httpclient.execute(patch);
            if (execute.getStatusLine().getStatusCode() != Constants.HTTP_RESPONSE_CODE_200_OK) {
                throw new IllegalArgumentException(EntityUtils.toString(execute.getEntity()));
            }
        }
        log.info("Flush path " + fullPath + " success.");
    }

    public Response handleResponse(Response response) {
        log.info("[handleResponse] response:[{}] {}.", response.status(), response.headers());
        if (Constants.HTTP_RESPONSE_CODE_200_OK == response.status()
                || Constants.HTTP_RESPONSE_CODE_201_CREATED == response.status()
                || Constants.HTTP_RESPONSE_CODE_202_ACCEPTED == response.status()) {
            return response;
        } else {
            throw new IllegalArgumentException(response.status() + " : " + response.error(String.class));
        }
    }

}
