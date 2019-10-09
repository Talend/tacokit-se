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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.datastore.Token;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;

import javax.json.JsonObject;

@Service
@Version(1)
@Slf4j
@Data
public class WorkdayReaderService {

    @Service
    private WorkdayReader reader;

    @Service
    private AccessTokenService accessToken;

    public JsonObject find(WorkdayDataSet ds, int offset, int limit) {
        final Token token = accessToken.findToken(ds.getDatastore());
        final String authorizeHeader = token.getAuthorizationHeaderValue();

        this.reader.base(ds.getDatastore().getEndpoint());

        Response<JsonObject> result = reader.search(authorizeHeader, ds.getService(), offset, limit);

        if (result.status() / 100 != 2) {
            String errorLib = result.error(String.class);
            log.error("Error while retrieve data {} : HTTP {} : {}", ds.getService(), result.status(), errorLib);
            throw new WorkdayException(errorLib);
        }
        return result.body();
    }

}
