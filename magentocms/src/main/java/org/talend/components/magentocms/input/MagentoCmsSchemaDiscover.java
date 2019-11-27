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
package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Documentation("Schema discovering class")
@Service
public class MagentoCmsSchemaDiscover implements Serializable {

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    public List<String> getColumns(MagentoInputConfiguration configuration) {
        List<String> result = new ArrayList<>();

        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = configuration.getMagentoDataSet().getMagentoUrl();

        try {
            Iterator<JsonObject> dataArrayIterator = magentoHttpClientService
                    .getRecords(configuration.getMagentoDataSet().getMagentoDataStore(), magentoUrl, allParameters).iterator();
            if (dataArrayIterator.hasNext()) {
                JsonValue val = dataArrayIterator.next();
                val.asJsonObject().forEach((columnName, value) -> result.add(columnName));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }
}