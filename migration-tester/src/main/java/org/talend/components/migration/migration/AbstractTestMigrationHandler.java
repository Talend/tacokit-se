/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.migration.migration;

import org.talend.components.migration.conf.AbstractConfig;
import org.talend.sdk.component.api.component.MigrationHandler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public abstract class AbstractTestMigrationHandler implements MigrationHandler {

    private static String current;

    static {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        current = sdf.format(new Date());
    }

    public abstract String getPrefix();

    public abstract String getCallbackPropertyName();

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        incomingData.put(getPrefix() + getCallbackPropertyName(), getMigrationVersions(incomingVersion) + " | " + this.current);

        copyFromLegacyToDuplication(incomingData);

        return incomingData;
    }

    /**
     * Copy value from a legacy property to a another one (duplication).
     *
     * @param incomingData
     * @return incomingData
     */
    private Map<String, String> copyFromLegacyToDuplication(Map<String, String> incomingData) {
        final String legacy = incomingData.get(getPrefix() + getLegacy());
        incomingData.put(getPrefix() + getDuplication(), legacy);
        return incomingData;
    }

    protected abstract String getDuplication();

    protected abstract String getLegacy();

    private String getMigrationVersions(int incomingVersion) {
        return incomingVersion + " -> " + AbstractConfig.VERSION;
    }

}
