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
package org.talend.components.adlsgen2.service;

import java.io.InputStream;
import java.util.Map;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Headers;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.QueryParams;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.UseConfigurer;

public interface AdlsGen2APIClient extends HttpClient {

    // GET http://{accountName}.{dnsSuffix}/?resource=account

    /**
     * @return Filesystem[]
     */
    @Request(path = "/")
    Response<JsonObject> filesystemList( //
            @Headers() Map<String, String> headers, //
            @QueryParams(encode = false) Map<String, String> sas, //
            @Query("resource") String resource, //
            @Query("timeout") Integer timeout //
    );

    // GET http://{accountName}.{dnsSuffix}/?resource=account&prefix={prefix}&continuation={continuation}&maxResults
    // ={maxResults}&timeout={timeout}

    /**
     * @param prefix Filters results to filesystems within the specified prefix.
     * @param continuation The number of filesystems returned with each invocation is limited. If the number of filesystems to be
     * returned exceeds this limit, a continuation token is returned in the response header x-ms-continuation. When a continuation
     * token is returned in the response, it must be specified in a subsequent invocation of the list operation to continue
     * listing the filesystems.
     * @param maxResults An optional value that specifies the maximum number of items to return. If omitted or greater than 5,000,
     * the response will include up to 5,000 items.
     * @param timeout An optional operation timeout value in seconds. The period begins when the request is received by the
     * service. If the timeout value elapses before the operation completes, the operation fails.
     * @return Filesystem[]
     * <p>
     * Where Filesystem is
     * eTag string
     * lastModified string
     * name string
     */
    @UseConfigurer(AdlsGen2APIConfigurer.class)
    @Request(path = "/?resource=account")
    Response<JsonObject> filesystemList( //
            @Headers() Map<String, String> headers, //
            @Query("prefix") Boolean prefix, //
            @Query("continuation") String continuation, //
            @Query("maxResults") Integer maxResults, //
            @Query("timeout") Integer timeout //
    );

    // GET http://{accountName}.{dnsSuffix}/{filesystem}?directory={directory}&recursive={recursive}&continuation
    // ={continuation}&maxResults={maxResults}&upn={upn}&resource=filesystem&timeout={timeout}

    /**
     * @param filesystem The filesystem identifier. The value must start and end with a letter or number and must contain only
     * letters, numbers, and the dash (-) character. Consecutive dashes are not permitted. All letters must be lowercase. The
     * value must have between 3 and 63 characters.
     * Regex pattern: ^[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]$
     * @param directory Filters results to paths within the specified directory. An error occurs if the directory does not exist.
     * @param recursive If "true", all paths are listed; otherwise, only paths at the root of the filesystem are listed. If
     * "directory" is specified, the list will only include paths that share the same root.
     * @param continuation The number of paths returned with each invocation is limited. If the number of paths to be returned
     * exceeds this limit, a continuation token is returned in the response header x-ms-continuation. When a continuation token is
     * returned in the response, it must be specified in a subsequent invocation of the list operation to continue listing the
     * paths.
     * @param maxResults An optional value that specifies the maximum number of items to return. If omitted or greater than 5,000,
     * the response will include up to 5,000 items.
     * @param upn Optional. Valid only when Hierarchical Namespace is enabled for the account. If "true", the user identity values
     * returned in the owner and group fields of each list entry will be transformed from Azure Active Directory Object IDs to
     * User Principal Names. If "false", the values will be returned as Azure Active Directory Object IDs. The default value is
     * false. Note that group and application Object IDs are not translated because they do not have unique friendly names.
     * @param timeout An optional operation timeout value in seconds. The period begins when the request is received by the
     * service. If the timeout value elapses before the operation completes, the operation fails.
     * @return Path[]
     * <p>
     * Where Path is
     * contentLength integer
     * eTag string
     * group string
     * isDirectory boolean
     * lastModified string
     * name string
     * owner string
     * permissions string
     */
    @Request(path = "/{filesystem}")
    Response<JsonObject> pathList( //
            @Headers() Map<String, String> headers, //
            @Path("filesystem") String filesystem, //
            @QueryParams(encode = false) Map<String, String> sas, //
            @Query("directory") String directory, //
            @Query("resource") String resource, //
            @Query("recursive") Boolean recursive, //
            @Query("continuation") String continuation, //
            @Query("maxResults") Integer maxResults, //
            @Query("timeout") Integer timeout //
    );

    // GET http://{accountName}.{dnsSuffix}/{filesystem}/{path}?timeout={timeout}

    /**
     * @param filesystem The filesystem identifier.
     * @param path The file or directory path.
     * @param sas
     * @return
     */
    @Request(path = "/{filesystem}/{path}")
    Response<InputStream> pathRead( //
            @Headers() Map<String, String> headers, //
            @Path("filesystem") String filesystem, //
            @Path("path") String path, //
            @Query("timeout") Integer timeout, //
            @QueryParams(encode = false) Map<String, String> sas //
    );

    @Request(path = "/{filesystem}/{path}", method = "HEAD")
    Response<JsonObject> pathGetProperties( //
            @Headers() Map<String, String> headers, //
            @Path("filesystem") String filesystem, //
            @Path("path") String path, //
            @Query("timeout") Integer timeout, //
            @QueryParams(encode = false) Map<String, String> sas //
    );

    // PUT http://{accountName}.{dnsSuffix}/{filesystem}/{path}
    @Request(path = "/{filesystem}/{path}", method = "PUT")
    Response<JsonObject> pathCreate( //
            @Headers() Map<String, String> headers, //
            @Path("filesystem") String filesystem, //
            @Path("path") String path, //
            @Query("resource") String resource, //
            @Query("timeout") Integer timeout, //
            @QueryParams(encode = false) Map<String, String> sas, //
            String emptyPayload // As Content-Length is needed and it is a restricted header
    // that we cannot force, we set an empty body so content-length will be set.
    );

    // PATCH http://{accountName}.{dnsSuffix}/{filesystem}/{path}?action={action}

    /**
     * @param filesystem The filesystem identifier.
     * @param path The file or directory path.
     * @param action The action must be "append" to upload data to be appended to a file, "flush" to flush previously uploaded
     * data to a file, "setProperties" to set the properties of a file or directory, or "setAccessControl" to set the owner,
     * group, permissions, or access control list for a file or directory.
     * @param sas Shared Acccess Signature
     * @param payload File content
     * @return
     */
    @Request(path = "/{filesystem}/{path}", method = "PATCH")
    Response<JsonObject> pathUpdate( //
            @Headers() Map<String, String> headers, //
            @Path("filesystem") String filesystem, //
            @Path("path") String path, //
            @Query("action") String action, //
            @Query("position") long position, //
            @Query("timeout") Integer timeout, //
            @QueryParams(encode = false) Map<String, String> sas, //
            byte[] payload //
    );

}
