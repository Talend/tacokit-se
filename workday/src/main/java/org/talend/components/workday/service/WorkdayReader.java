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
package org.talend.components.workday.service;

import org.talend.sdk.component.api.service.http.*;

import javax.json.JsonObject;

/**
 * Service data getter from workday.
 */
public interface WorkdayReader extends HttpClient {

    /**
     *
     * @param token : authorize token.
     * @param path : final path for workday REST service (common/v1/workers ...).
     * @param offset : The offset in the collection.
     * @param limit : The number of instances to retrieve.
     * @return
     */
    @Request(path = "/{service}", method = "GET")
    Response<JsonObject> search(@Header("Authorization") String token, @Path("service") String servicePath,
            @Query("offset") int offset, @Query("limit") int limit);
}
