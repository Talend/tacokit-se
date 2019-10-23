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
import org.talend.components.workday.dataset.QueryHelper;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.http.Response;

import javax.json.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
@Version(1)
@Slf4j
@Data
public class WorkdayReaderService {

    @Service
    private WorkdayReader reader;

    @Service
    private AccessTokenService accessToken;

    public JsonObject find(QueryHelper ds, Map<String, String> queryParams) {
        final Token token = accessToken.findToken(ds.getDatastore());
        final String authorizeHeader = token.getAuthorizationHeaderValue();

        this.reader.base(ds.getDatastore().getEndpoint());

        final String serviceToCall = ds.getServiceToCall();

        Response<JsonObject> result = reader.search(authorizeHeader, serviceToCall, queryParams);

        if (result.status() / 100 != 2) {
            String errorLib = result.error(String.class);
            log.error("Error while retrieve data {} : HTTP {} : {}", serviceToCall, result.status(), errorLib);
            throw new WorkdayException(errorLib);
        }
        return result.body();
    }

    public JsonObject findPage(QueryHelper ds, int offset, int limit, Map<String, String> queryParams) {
        Map<String, String> allQueryParams = new HashMap<>();
        if (queryParams != null) {
            allQueryParams.putAll(queryParams);
        }
        allQueryParams.put("offset", Integer.toString(offset));
        allQueryParams.put("limit", Integer.toString(limit));
        return this.find(ds, allQueryParams);
    }

    @Suggestions("workdayServices")
    public SuggestionValues loadServices(@Option WorkdayDataStore datastore) {
        return new SuggestionValues(true,
                Arrays.asList(new SuggestionValues.Item("1", "/workers"), new SuggestionValues.Item("2", "/toto")));
    }

}
