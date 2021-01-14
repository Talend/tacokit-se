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

    public void specificMigration(int incomingVersion, Map<String, String> incomingData) {
    };

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        copyConfig(this.getIncoming(), incomingData);

        incomingData.put(getPrefix() + getCallbackPropertyName(), getMigrationVersions(incomingVersion) + " | " + this.current);

        copyFromLegacyToDuplication(incomingData);

        copyConfig(this.getOutgoing(), incomingData);

        this.from(incomingData);

        specificMigration(incomingVersion, incomingData);

        return incomingData;
    }

    private void copyConfig(final String property, final Map<String, String> incomingData) {

        final StringBuilder sb = new StringBuilder();
        sb.append("{").append("\n");
        incomingData.entrySet().stream().forEach(e -> {
            if (sb.length() > 2) {
                sb.append(",\n");
            }
            sb.append("\t").append("\"").append(e.getKey()).append("\" : ").append("\"").append(e.getValue()).append("\"");
        });
        sb.append("\n}");

        incomingData.put(getPrefix() + property, sb.toString());
    }

    /**
     * Copy value from a legacy property to a another one (duplication).
     *
     * @param incomingData
     * @return incomingData
     */
    private void copyFromLegacyToDuplication(Map<String, String> incomingData) {
        final String legacy = incomingData.get(getPrefix() + getLegacy());
        incomingData.put(getPrefix() + getDuplication(), legacy);
    }

    protected abstract String getDuplication();

    protected abstract String getLegacy();

    protected abstract String getIncoming();

    protected abstract String getOutgoing();

    protected abstract void from(final Map<String, String> incomingData);

    private String getMigrationVersions(int incomingVersion) {
        return incomingVersion + " -> " + AbstractConfig.VERSION;
    }

}
