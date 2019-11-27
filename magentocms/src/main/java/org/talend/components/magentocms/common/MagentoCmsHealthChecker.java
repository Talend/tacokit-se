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
package org.talend.components.magentocms.common;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Service
public class MagentoCmsHealthChecker implements Serializable {

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    public boolean checkHealth(MagentoDataStore dataStore)
            throws UnknownAuthenticationTypeException, IOException, BadRequestException, BadCredentialsException {
        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = "index.php/rest/" + dataStore.getMagentoRestVersion() + "/" + "products";

        magentoHttpClientService.getRecords(dataStore, magentoUrl, allParameters);
        return true;
    }
}