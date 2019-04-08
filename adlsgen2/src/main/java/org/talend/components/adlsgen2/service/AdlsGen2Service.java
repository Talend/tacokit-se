// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.adlsgen2.service;

import java.io.InputStream;
import java.io.Serializable;
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

import org.talend.components.adlsgen2.common.format.csv.CsvIterator;
import org.talend.components.adlsgen2.common.format.parquet.ParquetIterator;
import org.talend.components.adlsgen2.common.format.unknown.UnknownIterator;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.Constants;
import org.talend.components.adlsgen2.datastore.Constants.HeaderConstants;
import org.talend.components.adlsgen2.datastore.SharedKeyUtils;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.google.common.base.Splitter;
import com.microsoft.rest.v2.http.HttpHeaders;
import com.microsoft.rest.v2.http.HttpMethod;
import com.microsoft.rest.v2.http.HttpRequest;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AdlsGen2Service implements Serializable {

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
    private AccessTokenProvider accessTokenProvider;

    private transient String auth;

    private transient String sas;

    private transient Map<String, String> sasMap;

    public AdlsGen2APIClient getClient(@Configuration("connection") final AdlsGen2Connection connection) {
        log.warn("[getClient] setting base url {}", connection.apiUrl());
        client.base(connection.apiUrl());
        return client;
    }

    public void preprareRequest(@Configuration("connection") final AdlsGen2Connection connection) {
        client.base(connection.apiUrl());
        auth = "";
        switch (connection.getAuthMethod()) {
        case SharedKey:
            try {
                String now = Constants.RFC1123GMTDateFormatter.format(OffsetDateTime.now());
                String version = HeaderConstants.TARGET_STORAGE_VERSION;
                String contentType = HeaderConstants.DFS_CONTENT_TYPE;
                URL url = new URL(connection.apiUrl());
                Map<String, String> heads = new HashMap<>();
                heads.put(Constants.HeaderConstants.DATE, now);
                heads.put(HeaderConstants.CONTENT_TYPE, contentType);
                heads.put(HeaderConstants.VERSION, version);
                HttpHeaders headers = new HttpHeaders(heads);
                HttpRequest request = new HttpRequest(null, HttpMethod.GET, url, headers, null, null);
                auth = new SharedKeyUtils(connection.getAccountName(), connection.getSharedKey())
                        .buildAuthenticationSignature(request);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            break;
        case AccessToken:
            String token = getAccessToken(connection);
            auth = String.format(HeaderConstants.AUTH_BEARER, token);
            break;
        case SAS:
            sas = connection.getSas().substring(1);
            auth = String.format(HeaderConstants.AUTH_SHARED_ACCESS_SIGNATURE, sas);
            sasMap = Splitter.on("&").withKeyValueSeparator("=").split(sas);
            break;
        case ADAL:
            throw new UnsupportedOperationException("Authentication method unsupported");
        }
    }

    @SuppressWarnings("unchecked")
    private RuntimeException handleError(int status, Map<String, List<String>> headers) {
        StringBuilder sb = new StringBuilder("[" + status + "] ");
        List<String> errors = headers.get(HeaderConstants.HEADER_X_MS_ERROR_CODE);
        if (errors != null && errors.isEmpty()) {
            for (String error : errors) {
                sb.append(error);
                if (ApiErrors.valueOf(error) != null) {
                    sb.append(": " + ApiErrors.valueOf(error));
                }
                sb.append("\n");
            }
        }
        log.error("[handleResponse] {}", sb.toString());
        return new RuntimeException(sb.toString());
    }

    public Response handleResponse(Response response) {
        log.info("[handleResponse] response:[{}] {}.", response.status(), response.headers());
        if (successfulOperations.contains(response.status())) {
            return response;
        } else {
            throw handleError(response.status(), response.headers());
        }
    }

    public Iterator<Record> convertToRecordList(@Configuration("dataSet") final AdlsGen2DataSet dataSet, InputStream content) {
        log.warn("[convertToRecordList] type: {}", content.getClass().getName());
        switch (dataSet.getFormat()) {
        case CSV:
            return CsvIterator.Builder.of().withConfiguration(dataSet.getCsvConfiguration()).parse(content);
        case AVRO:
        case JSON:
            throw new IllegalArgumentException("Not implemented");
        case PARQUET:
            log.warn("[convertToRecordList] PARQUET");
            return ParquetIterator.Builder.of().withConfiguration(dataSet.getParquetConfiguration()).parse(content);
        case UNKNOWN:
            return UnknownIterator.Builder.of().withConfiguration(dataSet.getUnknownConfiguration()).parse(content);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public String getAccessToken(@Configuration("connection") final AdlsGen2Connection connection) {
        accessTokenProvider.base(connection.oauthUrl());
        String payload = String.format(Constants.TOKEN_FORM, connection.getClientId(), connection.getClientSecret());
        Response<JsonObject> token = handleResponse(accessTokenProvider.getAccessToken(connection.getTenantId(), payload));
        return token.body().getString(Constants.ATTR_ACCESS_TOKEN);
    }

    @SuppressWarnings("unchecked")
    public List<String> filesystemList(@Configuration("connection") final AdlsGen2Connection connection) {
        preprareRequest(connection);
        Response<JsonObject> result = handleResponse(client.filesystemList(connection, auth, sasMap, Constants.ATTR_ACCOUNT));
        List<String> fs = new ArrayList<>();
        for (JsonValue v : result.body().getJsonArray(Constants.ATTR_FILESYSTEMS)) {
            fs.add(v.asJsonObject().getString(Constants.ATTR_NAME));
        }
        log.info("fs: {}", fs);
        return fs;
    }

    @SuppressWarnings("unchecked")
    public JsonArray pathList(@Configuration("configuration") final InputConfiguration configuration) {
        preprareRequest(configuration.getDataSet().getConnection());
        Response<JsonObject> result = handleResponse(client.pathList( //
                configuration.getDataSet().getConnection(), //
                auth, //
                configuration.getDataSet().getFilesystem(), //
                sasMap, //
                configuration.getDataSet().getBlobPath(), //
                Constants.ATTR_FILESYSTEM, //
                true, //
                null, //
                5000, //
                "", //
                60 //
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
    public Map<String, String> pathGetProperties(@Configuration("dataSet") final AdlsGen2DataSet dataSet) {
        preprareRequest(dataSet.getConnection());
        Map<String, String> properties = new HashMap<>();
        Response<JsonObject> result = handleResponse(client.pathGetProperties( //
                dataSet.getConnection(), //
                auth, //
                dataSet.getFilesystem(), //
                dataSet.getBlobPath(), //
                sasMap //
        ));
        log.warn("[pathGetProperties] [{}] {}.\n{}", result.status(), result.headers());
        if (result.status() == 200) {
            for (String header : result.headers().keySet()) {
                if (header.startsWith(Constants.PREFIX_FOR_STORAGE_HEADER)) {
                    properties.put(header, result.headers().get(header).toString());
                }
            }
        }
        return properties;
    }

    public BlobInformations getBlobInformations(@Configuration("dataSet") final AdlsGen2DataSet dataSet) {
        preprareRequest(dataSet.getConnection());
        BlobInformations infos = new BlobInformations();
        Response<JsonObject> result = client.pathList( //
                dataSet.getConnection(), //
                auth, //
                dataSet.getFilesystem(), //
                sasMap, //
                extractFolderPath(dataSet.getBlobPath()), //
                // dataSet.getBlobPath(), //
                Constants.ATTR_FILESYSTEM, //
                false, //
                null, //
                5000, //
                "", //
                60 //
        );
        log.warn("[pathExists] [{}] {}.\n{}", result.status(), result.headers(), result.body());
        if (result.status() != Constants.HTTP_RESPONSE_CODE_200_OK) {
            return infos;
        }
        String fileName = extractFileName(dataSet.getBlobPath());
        for (JsonValue f : result.body().getJsonArray(Constants.ATTR_PATHS)) {
            log.info("[pathExists] => {}.", f.asJsonObject().getString(Constants.ATTR_NAME));
            if (f.asJsonObject().getString(Constants.ATTR_NAME).equals(dataSet.getBlobPath())) {
                infos.setExists(true);
                infos.setName(f.asJsonObject().getString(Constants.ATTR_NAME));
                infos.setFileName(fileName);
                infos.setPath(extractFolderPath(dataSet.getBlobPath()));
                infos.setEtag(f.asJsonObject().getString("etag"));
                infos.setContentLength(Integer.parseInt(f.asJsonObject().getString("contentLength")));
                infos.setLastModified(f.asJsonObject().getString("lastModified"));
                infos.setOwner(f.asJsonObject().getString("owner"));
                infos.setPermissions(f.asJsonObject().getString("permissions"));
            }
        }

        return infos;
    }

    public Boolean pathExists(@Configuration("dataSet") final AdlsGen2DataSet dataSet) {
        return getBlobInformations(dataSet).isExists();
    }

    @SuppressWarnings("unchecked")
    public Iterator<Record> pathRead(@Configuration("configuration") final InputConfiguration configuration) {
        preprareRequest(configuration.getDataSet().getConnection());
        Response<InputStream> result = handleResponse(client.pathRead( //
                configuration.getDataSet().getConnection(), //
                auth, //
                configuration.getDataSet().getFilesystem(), //
                configuration.getDataSet().getBlobPath(), //
                60, //
                sasMap //
        ));
        log.warn("[pathRead] [{}] {}.", result.status(), result.headers());
        return convertToRecordList(configuration.getDataSet(), result.body());
    }

    @SuppressWarnings("unchecked")
    public Response<JsonObject> pathCreate(@Configuration("configuration") final OutputConfiguration configuration) {
        preprareRequest(configuration.getDataSet().getConnection());
        Response<JsonObject> result = handleResponse(client.pathCreate( //
                configuration.getDataSet().getConnection(), //
                auth, //
                configuration.getDataSet().getFilesystem(), //
                configuration.getDataSet().getBlobPath(), //
                Constants.ATTR_FILE, //
                sasMap, //
                ""));
        log.warn("[pathCreate] [{}] {}.\n{}", result.status(), result.headers(), result.body());
        return result;
    }

    @SuppressWarnings("unchecked")
    public Response<JsonObject> pathUpdate(@Configuration("configuration") final OutputConfiguration configuration,
            String content, long position) {
        preprareRequest(configuration.getDataSet().getConnection());
        Response<JsonObject> result = handleResponse(client.pathUpdate( //
                configuration.getDataSet().getConnection(), //
                auth, //
                configuration.getDataSet().getFilesystem(), //
                configuration.getDataSet().getBlobPath(), //
                Constants.ATTR_ACTION_APPEND, //
                position, //
                sasMap, //
                content //
        ));
        log.warn("[pathUpdate] [{}] {}", result.status(), result.headers());

        return result;
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
    public Response<JsonObject> flushBlob(@Configuration("configuration") OutputConfiguration configuration, long position) {
        Response<JsonObject> result;
        result = handleResponse(client.pathUpdate( //
                configuration.getDataSet().getConnection(), //
                auth, //
                configuration.getDataSet().getFilesystem(), //
                configuration.getDataSet().getBlobPath(), //
                Constants.ATTR_ACTION_FLUSH, //
                position, //
                sasMap, //
                "" //
        ));
        log.warn("[flushBlob] [{}] {}", result.status(), result.headers());

        return result;
    }

    /*
     * {"contentLength":"21","etag":"Mon, 25 Mar 2019 15:35:47 GMT","group":"$superuser","lastModified":"Mon, 25 Mar 2019
     * 15:35:47 GMT","name":"myNewFolder/customer.csv","owner":"$superuser","permissions":"rw-r-----"}
     */
    @Data
    @ToString
    public class BlobInformations {

        public String etag;

        public String group;

        public String lastModified;

        public String name;

        public String owner;

        public String permissions;

        private boolean exists = Boolean.FALSE;

        private String path;

        private String fileName;

        private Integer contentLength = 0;
    }
}
