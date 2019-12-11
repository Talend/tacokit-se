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
package org.talend.components.marketo.dataset;

import java.util.Map;

import org.talend.components.marketo.dataset.MarketoDataSet.DateTimeRelative;
import org.talend.sdk.component.api.component.MigrationHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MarketoDataSetMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        if (incomingVersion < 2) {
            log.info("[migrate] actual version: {}. data: {}.", incomingVersion, incomingData);
            String dateTimeRelativeOffset = incomingData.get("configuration.sinceDateTimeRelative");
            if ("14".equals(dateTimeRelativeOffset)) {
                incomingData.put("configuration.sinceDateTimeRelative", DateTimeRelative.PERIOD_AGO_2W.getRelativeOffset());
                incomingData.remove("configuration.sinceDateTimeRelative_name");
                log.debug("[migrate] returning: {}.", incomingData);
            }
        }
        return incomingData;
    }

}
