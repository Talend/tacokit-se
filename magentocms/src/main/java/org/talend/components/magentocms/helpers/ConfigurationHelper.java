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
package org.talend.components.magentocms.helpers;

import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.output.MagentoOutputConfiguration;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class ConfigurationHelper {

    public static final String DATA_STORE_ID = "MagentoDataStore";

    public static final String DATA_SET_INPUT_ID = "MagentoInput";

    public static final String DATA_SET_OUTPUT_ID = "MagentoOutput";

    public static final String DATA_STORE_HEALTH_CHECK = "DataStoreHealthCheck";

    public static final String DISCOVER_SCHEMA_INPUT_ID = "guessTableSchema";

    public static final String VALIDATE_WEB_SERVER_URL_ID = "validateWebServerUrl";

    public static final String VALIDATE_AUTH_LOGIN_PASSWORD_LOGIN_ID = "validateAuthLogin";

    public static final String VALIDATE_AUTH_OAUTH_PARAMETER_ID = "validateAuthOauthParameters";

    public static final String VALIDATE_AUTH_TOKEN_ID = "validateAuthToken";

    public static final String UPDATABLE_FILTER_ADVANCED_ID = "updatableFilterAdvanced";

    public static void fillFilterParameters(Map<String, String> allParameters, ConfigurationFilter filterConfiguration,
            boolean encodeValue) throws UnsupportedEncodingException {
        Map<Integer, Integer> filterIds = new HashMap<>();
        if (filterConfiguration != null) {
            int groupId = 0;
            for (SelectionFilter filter : filterConfiguration.getFilterLines()) {
                Integer filterId = filterIds.get(groupId);
                if (filterId == null) {
                    filterId = 0;
                } else {
                    filterId++;
                }
                filterIds.put(groupId, filterId);

                allParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][field]",
                        filter.getFieldName());
                allParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][condition_type]",
                        filter.getFieldNameCondition());
                allParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][value]",
                        encodeValue ? URLEncoder.encode(filter.getValue(), "UTF-8") : filter.getValue());

                if (filterConfiguration.getFilterOperator() == SelectionFilterOperator.AND) {
                    groupId++;
                }
            }
        }
    }

    public static void setupServicesInput(MagentoInputConfiguration configuration,
            MagentoHttpClientService magentoHttpClientService) {
        magentoHttpClientService.setBase(configuration.getMagentoDataSet().getMagentoDataStore().getMagentoWebServerUrl());
    }

    public static void setupServicesOutput(MagentoOutputConfiguration configuration,
            MagentoHttpClientService magentoHttpClientService) {
        magentoHttpClientService.setBase(configuration.getMagentoDataSet().getMagentoDataStore().getMagentoWebServerUrl());
    }
}
