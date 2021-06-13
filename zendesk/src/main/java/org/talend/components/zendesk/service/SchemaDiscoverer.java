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
package org.talend.components.zendesk.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.helpers.CommonHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.source.ZendeskInputMapperConfiguration;
import org.talend.components.zendesk.source.InputIterator;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class SchemaDiscoverer implements Serializable {

    @Service
    private Messages i18n;

    @Service
    private ZendeskHttpClientService httpClientService;

    public List<String> getColumns(ZendeskInputMapperConfiguration configuration) {
        List<String> result = new ArrayList<>();
        try {
            InputIterator itemIterator = CommonHelper.getInputIterator(httpClientService, configuration, i18n);

            if (itemIterator.hasNext()) {
                JsonValue val = itemIterator.next();
                val.asJsonObject().forEach((columnName, value) -> result.add(columnName));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }
}