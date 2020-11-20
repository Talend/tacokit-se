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

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.adlsgen2.common.format.avro.AvroIterator;
import org.talend.components.adlsgen2.common.format.csv.CsvIterator;
import org.talend.components.adlsgen2.common.format.json.JsonIterator;
import org.talend.components.adlsgen2.common.format.parquet.ParquetIterator;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.Constants;
import org.talend.components.adlsgen2.datastore.Constants.HeaderConstants;
import org.talend.components.adlsgen2.datastore.Constants.MethodConstants;
import org.talend.components.adlsgen2.datastore.SharedKeyUtils;
import org.talend.components.adlsgen2.runtime.AdlsDatasetRuntimeInfo;
import org.talend.components.adlsgen2.runtime.AdlsDatastoreRuntimeInfo;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AdlsGen2Service {

    private static final Set<Integer> successfulOperations = new HashSet<>(Arrays.asList(Constants.HTTP_RESPONSE_CODE_200_OK,
            Constants.HTTP_RESPONSE_CODE_201_CREATED, Constants.HTTP_RESPONSE_CODE_202_ACCEPTED));

    // reflexion hack to support PATCH method.
    static {
        SupportPatch.allowMethods("PATCH");
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    }

    @Service
    private JsonBuilderFactory jsonFactory;

    @Service
    private RecordBuilderFactory recordBuilder;

    @Service
    private AdlsGen2APIClient client;

    @Service
    private AdlsActiveDirectoryService tokenProviderService;

    public AdlsGen2APIClient getClient(@Configuration("connection") final AdlsGen2Connection connection) {
        setDefaultRequestParameters(connection);
        return client;
    }

    private void setDefaultRequestParameters(final AdlsGen2Connection connection) {
        client.base(connection.apiUrl());
    }

    private Map<String, String> prepareRequestHeaders(final Map<String, String> secretsMap, final AdlsGen2Connection connection,
            String url, String method, String payloadLength) {
        log.debug("[prepareRequest] {} [{}].", url, method);
        secretsMap.put(HeaderConstants.USER_AGENT, HeaderConstants.USER_AGENT_AZURE_DLS_GEN2);
        secretsMap.put(Constants.HeaderConstants.DATE, Constants.RFC1123GMTDateFormatter.format(OffsetDateTime.now()));
        secretsMap.put(HeaderConstants.CONTENT_TYPE, HeaderConstants.DFS_CONTENT_TYPE);
        secretsMap.put(HeaderConstants.VERSION, HeaderConstants.TARGET_STORAGE_VERSION);
        if (StringUtils.isNotEmpty(payloadLength)) {
            secretsMap.put(HeaderConstants.CONTENT_LENGTH, payloadLength);
        } else {
            secretsMap.remove(HeaderConstants.CONTENT_LENGTH);
        }

        if (connection.getAuthMethod().equals(AdlsGen2Connection.AuthMethod.SharedKey)) {
            try {
                URL dest = new URL(url);
                String auth = new SharedKeyUtils(connection.getAccountName(), connection.getSharedKey())
                        .buildAuthenticationSignature(dest, method, secretsMap);
                secretsMap.put(HeaderConstants.AUTHORIZATION, auth);
            } catch (Exception e) {
                log.error("[prepareRequest] {}", e.getMessage());
                throw new AdlsGen2RuntimeException(e.getMessage());
            }
        }

        return secretsMap;
    }

    @SuppressWarnings("unchecked")
    private static AdlsGen2RuntimeException handleError(final int status, final Map<String, List<String>> headers) {
        return handleError(status, headers, null);
    }

    private static AdlsGen2RuntimeException handleError(final int status, final Map<String, List<String>> headers,
            String errorMessage) {
        StringBuilder sb = new StringBuilder();
        List<String> errors = headers.get(HeaderConstants.HEADER_X_MS_ERROR_CODE);
        if (errors != null && !errors.isEmpty()) {
            for (String error : errors) {
                sb.append(error);
                sb.append(" [" + status + "]");
                try {
                    sb.append(": " + ApiErrors.valueOf(error));
                } catch (IllegalArgumentException e) {
                    // could not find an api detailed message
                }
                sb.append(".\n");
            }
        } else if (errorMessage != null) {
            sb.append(" HTTP status: " + status + ". Error message: ").append(errorMessage);
        } else {
            sb.append("No error code provided. HTTP status:" + status + ".");
        }
        log.error("[handleResponse] {}", sb);
        return new AdlsGen2RuntimeException(sb.toString());
    }

    public static Response handleResponse(Response response) {
        if (successfulOperations.contains(response.status())) {
            return response;
        } else {
            String errorMessage = (String) response.error(String.class);
            if (errorMessage != null) {
                throw handleError(response.status(), response.headers(), errorMessage);
            } else {
                throw handleError(response.status(), response.headers());
            }
        }
    }

    public Iterator<Record> convertToRecordList(@Configuration("dataSet") final AdlsGen2DataSet dataSet, InputStream content) {
        switch (dataSet.getFormat()) {
        case CSV:
            return CsvIterator.Builder.of(recordBuilder).withConfiguration(dataSet.getCsvConfiguration()).parse(content);
        case AVRO:
            return AvroIterator.Builder.of(recordBuilder).withConfiguration(dataSet.getAvroConfiguration()).parse(content);
        case JSON:
            return JsonIterator.Builder.of(recordBuilder, jsonFactory).withConfiguration(dataSet.getJsonConfiguration())
                    .parse(content);
        case PARQUET:
            return ParquetIterator.Builder.of(recordBuilder).withConfiguration(dataSet.getParquetConfiguration()).parse(content);
        }
        throw new AdlsGen2RuntimeException("Could not determine operation to do.");
    }

    @SuppressWarnings("unchecked")
    public List<String> filesystemList(final AdlsDatastoreRuntimeInfo connectionRuntimeInfo) {
        setDefaultRequestParameters(connectionRuntimeInfo.getConnection());
        String url = String.format("%s/?resource=account&timeout=%d", connectionRuntimeInfo.getConnection().apiUrl(),
                connectionRuntimeInfo.getConnection().getTimeout());
        Map<String, String> headers = prepareRequestHeaders(connectionRuntimeInfo.getAdTokenMap(),
                connectionRuntimeInfo.getConnection(), url, MethodConstants.GET, "");
        Response<JsonObject> result = handleResponse(client.filesystemList(headers, connectionRuntimeInfo.getSASMap(),
                Constants.ATTR_ACCOUNT, connectionRuntimeInfo.getConnection().getTimeout()));
        List<String> fs = new ArrayList<>();
        for (JsonValue v : result.body().getJsonArray(Constants.ATTR_FILESYSTEMS)) {
            fs.add(v.asJsonObject().getString(Constants.ATTR_NAME));
        }
        return fs;
    }

    @SuppressWarnings("unchecked")
    public JsonArray pathList(final AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        setDefaultRequestParameters(datasetRuntimeInfo.getDataSet().getConnection());
        String rcfmt = "%s/%s?directory=%s&resource=filesystem&recursive=false&maxResults=5000&timeout=%d";
        String url = getUrlStringWithoutPosition(datasetRuntimeInfo, rcfmt);
        log.debug("[pathList] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getDataSet().getConnection(), url, MethodConstants.GET, "");
        Response<JsonObject> result = handleResponse(client.pathList( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getSASMap(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                Constants.ATTR_FILESYSTEM, //
                false, //
                null, //
                5000, //
                datasetRuntimeInfo.getDataSet().getConnection().getTimeout() //
        ));
        return result.body().getJsonArray(Constants.ATTR_PATHS);
    }

    public String extractFolderPath(String blobPath) {
        Path path = Paths.get(blobPath);
        log.debug("[extractFolderPath] blobPath: {}. Path: {}. {}", blobPath, path.toString(), path.getNameCount());
        if (path.getNameCount() == 1) {
            return "/";
        }
        return Paths.get(blobPath).getParent().toString();
    }

    public String extractFileName(String blobPath) {
        Path path = Paths.get(blobPath);
        log.debug("[extractFileName] blobPath: {}. Path: {}. {}", blobPath, path.toString(), path.getNameCount());
        return Paths.get(blobPath).getFileName().toString();
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> pathGetProperties(final AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s/%s?timeout=%d";
        String url = String.format(rcfmt, //
                datasetRuntimeInfo.getConnection().apiUrl(), //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                datasetRuntimeInfo.getConnection().getTimeout() //
        );
        log.debug("[pathGetProperties] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.HEAD, "");
        Map<String, String> properties = new HashMap<>();
        Response<JsonObject> result = handleResponse(client.pathGetProperties( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                datasetRuntimeInfo.getConnection().getTimeout(), //
                datasetRuntimeInfo.getSASMap() //
        ));
        if (result.status() == 200) {
            for (String header : result.headers().keySet()) {
                if (header.startsWith(Constants.PREFIX_FOR_STORAGE_HEADER)) {
                    properties.put(header, result.headers().get(header).toString());
                }
            }
        }
        return properties;
    }

    public List<BlobInformations> getBlobs(final AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s?directory=%s&resource=filesystem&recursive=false&maxResults=5000&timeout=%d";
        String url = getUrlStringWithoutPosition(datasetRuntimeInfo, rcfmt);
        log.debug("[getBlobs] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.GET, "");
        Response<JsonObject> result = handleResponse(client.pathList( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getSASMap(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                Constants.ATTR_FILESYSTEM, //
                false, //
                null, //
                5000, //
                datasetRuntimeInfo.getConnection().getTimeout() //
        ));
        if (result.status() != Constants.HTTP_RESPONSE_CODE_200_OK) {
            log.error("[getBlobs] Invalid request [{}] {}", result.status(), result.headers());
            return new ArrayList<>();
        }
        List<BlobInformations> blobs = new ArrayList<>();
        for (JsonValue f : result.body().getJsonArray(Constants.ATTR_PATHS)) {
            if (f.asJsonObject().getOrDefault(Constants.ATTR_IS_DIRECTORY, JsonValue.NULL) == JsonValue.NULL) {
                BlobInformations infos = new BlobInformations();
                infos.setExists(true);
                String name = f.asJsonObject().getString(Constants.ATTR_NAME);
                infos.setName(name);
                infos.setFileName(extractFileName(name));
                infos.setBlobPath(name);
                infos.setDirectory(extractFolderPath(name));
                infos.setEtag(f.asJsonObject().getString("etag"));
                infos.setContentLength(Integer.parseInt(f.asJsonObject().getString("contentLength")));
                infos.setLastModified(f.asJsonObject().getString("lastModified"));
                if (f.asJsonObject().entrySet().contains("owner")) {
                    infos.setOwner(f.asJsonObject().getString("owner"));
                }
                if (f.asJsonObject().entrySet().contains("permissions")) {
                    infos.setPermissions(f.asJsonObject().getString("permissions"));
                }
                blobs.add(infos);
            }
        }
        log.debug("[getBlobs] blobs count {}.", blobs.size());

        return blobs;
    }

    public BlobInformations getBlobInformations(final AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s?directory=%s&resource=filesystem&recursive=false&maxResults=5000&timeout=%d";
        String url = String.format(rcfmt, //
                datasetRuntimeInfo.getConnection().apiUrl(), //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                extractFolderPath(datasetRuntimeInfo.getDataSet().getBlobPath()), //
                datasetRuntimeInfo.getConnection().getTimeout() //
        );
        log.debug("[getBlobInformations] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.GET, "");
        BlobInformations infos = new BlobInformations();
        Response<JsonObject> result = client.pathList( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getSASMap(), //
                extractFolderPath(datasetRuntimeInfo.getDataSet().getBlobPath()), //
                Constants.ATTR_FILESYSTEM, //
                false, //
                null, //
                5000, //
                datasetRuntimeInfo.getConnection().getTimeout() //
        );
        if (result.status() != Constants.HTTP_RESPONSE_CODE_200_OK) {
            log.debug("[getBlobInformations] blob info: {}", infos);
            return infos;
        }
        String fileName = extractFileName(datasetRuntimeInfo.getDataSet().getBlobPath());
        for (JsonValue f : result.body().getJsonArray(Constants.ATTR_PATHS)) {
            if (f.asJsonObject().getString(Constants.ATTR_NAME).equals(datasetRuntimeInfo.getDataSet().getBlobPath())) {
                infos.setExists(true);
                infos.setName(f.asJsonObject().getString(Constants.ATTR_NAME));
                infos.setFileName(fileName);
                infos.setBlobPath(extractFolderPath(datasetRuntimeInfo.getDataSet().getBlobPath()));
                infos.setEtag(f.asJsonObject().getString("etag"));
                infos.setContentLength(Integer.parseInt(f.asJsonObject().getString("contentLength")));
                infos.setLastModified(f.asJsonObject().getString("lastModified"));
                if (f.asJsonObject().entrySet().contains("owner")) {
                    infos.setOwner(f.asJsonObject().getString("owner"));
                }
                if (f.asJsonObject().entrySet().contains("permissions")) {
                    infos.setPermissions(f.asJsonObject().getString("permissions"));
                }
            }
        }
        log.debug("[getBlobInformations] blob meta: {}", infos);
        return infos;
    }

    public boolean blobExists(AdlsDatasetRuntimeInfo datasetRuntimeInfo, String blobName) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s?directory=%s&resource=filesystem&recursive=false&maxResults=5000&timeout=%d";
        String url = String.format(rcfmt, //
                datasetRuntimeInfo.getConnection().apiUrl(), //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                extractFolderPath(blobName), datasetRuntimeInfo.getConnection().getTimeout() //
        );
        log.debug("[blobExists] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.GET, "");
        BlobInformations infos = new BlobInformations();
        Response<JsonObject> result = client.pathList( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getSASMap(), //
                extractFolderPath(blobName), Constants.ATTR_FILESYSTEM, //
                false, //
                null, //
                5000, //
                datasetRuntimeInfo.getDataSet().getConnection().getTimeout() //
        );
        if (result.status() != Constants.HTTP_RESPONSE_CODE_200_OK) {
            log.debug("[blobExists] blob info: {}", infos);
            return false;
        }
        for (JsonValue f : result.body().getJsonArray(Constants.ATTR_PATHS)) {
            if (f.asJsonObject().getString(Constants.ATTR_NAME).equals(blobName)) {
                log.debug("[blobExists] Blob found");
                return true;
            }
        }
        log.debug("[blobExists] Blob NOT found");
        return false;
    }

    public Boolean pathExists(AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        return getBlobInformations(datasetRuntimeInfo).isExists();
    }

    @SuppressWarnings("unchecked")
    public Iterator<Record> pathRead(AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s/%s?timeout=%d";
        String url = getUrlStringWithoutPosition(datasetRuntimeInfo, rcfmt);
        log.debug("[pathRead] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.GET, "");
        Response<InputStream> result = handleResponse(client.pathRead( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                datasetRuntimeInfo.getConnection().getTimeout(), //
                datasetRuntimeInfo.getSASMap() //
        ));
        return convertToRecordList(datasetRuntimeInfo.getDataSet(), result.body());
    }

    @SuppressWarnings("unchecked")
    public InputStream getBlobInputstream(AdlsDatasetRuntimeInfo datasetRuntimeInfo, BlobInformations blob) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s/%s?timeout=%d";
        String url = String.format(rcfmt, //
                datasetRuntimeInfo.getDataSet().getConnection().apiUrl(), //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                blob.getBlobPath(), //
                datasetRuntimeInfo.getConnection().getTimeout() //
        );
        log.debug("[getBlobInputstream] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.GET, "");
        Response<InputStream> result = handleResponse(client.pathRead( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                blob.getBlobPath(), //
                datasetRuntimeInfo.getConnection().getTimeout(), //
                datasetRuntimeInfo.getSASMap() //
        ));
        return result.body();
    }

    @SuppressWarnings("unchecked")
    public Response<JsonObject> pathCreate(AdlsDatasetRuntimeInfo datasetRuntimeInfo) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s/%s?resource=file&timeout=%d";
        String url = getUrlStringWithoutPosition(datasetRuntimeInfo, rcfmt);
        log.debug("[pathCreate] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.PUT, "");
        return handleResponse(client.pathCreate( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                Constants.ATTR_FILE, //
                datasetRuntimeInfo.getConnection().getTimeout(), //
                datasetRuntimeInfo.getSASMap(), //
                ""));
    }

    @SuppressWarnings("unchecked")
    public Response<JsonObject> pathUpdate(AdlsDatasetRuntimeInfo datasetRuntimeInfo, byte[] content, long position) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s/%s?action=append&position=%s&timeout=%d";
        String url = getUrlStringWithPosition(datasetRuntimeInfo, position, rcfmt);
        log.debug("[pathUpdate] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.PATCH, String.valueOf(content.length));
        return handleResponse(client.pathUpdate( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                Constants.ATTR_ACTION_APPEND, //
                position, //
                datasetRuntimeInfo.getDataSet().getConnection().getTimeout(), //
                datasetRuntimeInfo.getSASMap(), //
                content //
        ));
    }

    /**
     * To flush, the previously uploaded data must be contiguous, the position parameter must be specified and equal to the
     * length of the file after all data has been written, and there must not be a request entity body included with the
     * request.
     *
     * @param configuration
     * @param position
     * @return
     */
    @SuppressWarnings("unchecked")
    public Response<JsonObject> flushBlob(AdlsDatasetRuntimeInfo datasetRuntimeInfo, long position) {
        setDefaultRequestParameters(datasetRuntimeInfo.getConnection());
        String rcfmt = "%s/%s/%s?action=flush&position=%s&timeout=%d";
        String url = getUrlStringWithPosition(datasetRuntimeInfo, position, rcfmt);
        log.debug("[flushBlob#pathUpdate] {}", url);
        Map<String, String> headers = prepareRequestHeaders(datasetRuntimeInfo.getAdTokenMap(),
                datasetRuntimeInfo.getConnection(), url, MethodConstants.PATCH, "");
        return handleResponse(client.pathUpdate( //
                headers, //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                Constants.ATTR_ACTION_FLUSH, //
                position, //
                datasetRuntimeInfo.getConnection().getTimeout(), //
                datasetRuntimeInfo.getSASMap(), //
                new byte[0] //
        ));
    }

    private String getUrlStringWithPosition(AdlsDatasetRuntimeInfo datasetRuntimeInfo, long position, String rcfmt) {
        return String.format(rcfmt, //
                datasetRuntimeInfo.getDataSet().getConnection().apiUrl(), //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                position, //
                datasetRuntimeInfo.getConnection().getTimeout() //
        );
    }

    private String getUrlStringWithoutPosition(AdlsDatasetRuntimeInfo datasetRuntimeInfo, String rcfmt) {
        return String.format(rcfmt, //
                datasetRuntimeInfo.getConnection().apiUrl(), //
                datasetRuntimeInfo.getDataSet().getFilesystem(), //
                datasetRuntimeInfo.getDataSet().getBlobPath(), //
                datasetRuntimeInfo.getDataSet().getConnection().getTimeout() //
        );
    }
}
