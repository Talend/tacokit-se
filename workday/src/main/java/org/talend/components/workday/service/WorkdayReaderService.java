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
import org.talend.components.workday.dataset.Parameters;
import org.talend.components.workday.dataset.QueryHelper;
import org.talend.components.workday.dataset.SwaggerLoader;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.datastore.Token;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.update.Update;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Version(1)
@Slf4j
@Data
public class WorkdayReaderService {

    @Service
    private WorkdayReader reader;

    @Service
    private AccessTokenService accessToken;

    private final transient SwaggerLoader loader;

    public WorkdayReaderService() {
        final URL swaggersDirectory = WorkdayReaderService.class.getClassLoader().getResource("swaggers/");
        this.loader = new SwaggerLoader(swaggersDirectory.getPath());
    }

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
        final Map<String, String> allQueryParams = new HashMap<>();
        if (queryParams != null) {
            allQueryParams.putAll(queryParams);
        }
        allQueryParams.put("offset", Integer.toString(offset));
        allQueryParams.put("limit", Integer.toString(limit));
        return this.find(ds, allQueryParams);
    }

    public Iterator<JsonObject> extractIterator(JsonObject result, String arrayName) {
        if (result == null) {
            return Collections.emptyIterator();
        }
        final String error = result.getString("error", null);
        if (error != null) {
            final JsonArray errors = result.getJsonArray("errors");
            throw new WorkdayException(error + " : " + errors.toString());
        }
        final JsonArray data = result.getJsonArray(arrayName);
        if (data == null || data.isEmpty()) {
            return Collections.emptyIterator();
        }
        return data.stream().map(JsonObject.class::cast).iterator();
    }

    @DynamicValues("workdayModules")
    public Values loadModules() {
        log.info("loadModules");
        return new Values(loader.getModules());
    }

    @Suggestions("workdayServices")
    public SuggestionValues loadServices(String module) {
        log.info("loadServices for module {}", module);
        final List<SuggestionValues.Item> services = loader.findGetServices(module).keySet().stream()
                .map((String service) -> new SuggestionValues.Item(service, service)).collect(Collectors.toList());

        return new SuggestionValues(false, services);
    }

    @Update("workdayServicesParams")
    public Parameters loadServiceParameter(String module, String service) {
        log.info("workdayServicesParams suggestion for {} {}", module, service);
        final Parameters parameters = new Parameters();
        parameters.setParameters(Collections.emptyList());
        final Map<String, List<WorkdayDataSet.Parameter>> moduleServices = this.loader.findGetServices(module);
        if (moduleServices == null) {
            return parameters;
        }
        final List<WorkdayDataSet.Parameter> serviceParameters = moduleServices.get(service);
        if (parameters == null) {
            return parameters;
        }
        log.info("workdayServicesParams : nombre params {}", parameters.getParameters().size());
        parameters.setParameters(serviceParameters);
        return parameters;
    }
}
