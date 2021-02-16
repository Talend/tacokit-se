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
package org.talend.components.common.service.adls;

import org.talend.components.common.connection.adls.AdlsGen2Connection;
import org.talend.sdk.component.api.service.http.*;

import javax.json.JsonObject;

import static org.talend.components.common.Constants.HeaderConstants.AUTHORIZATION;
import static org.talend.components.common.Constants.HeaderConstants.DATE;

public interface AdlsGen2APIClient extends HttpClient {

    // GET http://{accountName}.{dnsSuffix}/?resource=account
    /**
     * @return Filesystem[]
     */
    @UseConfigurer(AdlsGen2APIConfigurer.class)
    @Request(path = "/")
    Response<JsonObject> filesystemList( //
            @ConfigurerOption("connection") AdlsGen2Connection connection, //
            @Header(AUTHORIZATION) String auth, //
            @Header(DATE) String date, //
            @Query("resource") String resource);

    // GET http://{accountName}.{dnsSuffix}/filesystem?resource=filesystem&recursive=true
    /**
     * @return path[]
     */
    @UseConfigurer(AdlsGen2APIConfigurer.class)
    @Request(path = "/{filesystem}?resource=filesystem&recursive={recursive}&directory={directory}")
    Response<JsonObject> pathList( //
            @ConfigurerOption("connection") AdlsGen2Connection connection, //
            @Header(AUTHORIZATION) String auth, //
            @Header(DATE) String date, @Path("recursive") boolean recursive, @Path("filesystem") String filesystem,
            @Path("directory") String directory);

    // GET http://{accountName}.{dnsSuffix}/filesystem?resource=filesystem&recursive=true
    /**
     * @return path[]
     */
    @UseConfigurer(AdlsGen2APIConfigurer.class)
    @Request(path = "/{filesystem}/{path}?recursive=true", method = "DELETE")
    Response<JsonObject> pathDelete( //
            @ConfigurerOption("connection") AdlsGen2Connection connection, //
            @Header(AUTHORIZATION) String auth, //
            @Header(DATE) String date, @Path("filesystem") String filesystem, @Path("path") String path);

    // PUT http://{accountName}.{dnsSuffix}/{filesystem}/{path}?resource=file
    @UseConfigurer(AdlsGen2APIConfigurer.class)
    @Request(path = "/{filesystem}/{path}?resource=file", method = "PUT")
    Response<JsonObject> pathCreate( //
            @ConfigurerOption("connection") AdlsGen2Connection connection, //
            @Header(AUTHORIZATION) String auth, //
            @Header(DATE) String date, //
            @Path("filesystem") String filesystem, //
            @Path("path") String path, //
            String emptyPayload // As Content-Length is needed and it is a restricted header
    // that we cannot force, we set an empty body so content-length will be set.
    );

    // PUT http://{accountName}.{dnsSuffix}/{filesystem}/{path}?action=append
    @UseConfigurer(AdlsGen2APIConfigurer.class)
    @Request(path = "/{filesystem}/{path}?action=append&position={position}", method = "POST")
    Response<JsonObject> pathAppend( //
            @ConfigurerOption("connection") AdlsGen2Connection connection, //
            @Header(AUTHORIZATION) String auth, //
            @Header(DATE) String date, //
            @Header("X-HTTP-Method-Override") String patch, // This header can be used in a POST request to “fake” other HTTP
            // methods
            @Path("filesystem") String filesystem, //
            @Path("path") String path, //
            @Path("position") String position, byte[] Payload // As Content-Length is needed and it is a restricted header
    // that we cannot force, we set an empty body so content-length will be set.
    );
}
