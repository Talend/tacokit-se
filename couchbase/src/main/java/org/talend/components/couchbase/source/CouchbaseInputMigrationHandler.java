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
package org.talend.components.couchbase.source;

import org.talend.sdk.component.api.component.MigrationHandler;

import java.util.Map;

public class CouchbaseInputMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        if (incomingVersion == 1) {
            boolean useN1QLQuery = Boolean.valueOf(incomingData.get("configuration.useN1QLQuery"));
            if (useN1QLQuery) {
                incomingData.put("configuration.selectAction", "N1QL");
            } else {
                incomingData.put("configuration.selectAction", "ALL");
            }
        }
        return incomingData;
    }
}
