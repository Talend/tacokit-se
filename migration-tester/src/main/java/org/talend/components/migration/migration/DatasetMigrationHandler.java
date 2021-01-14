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

import java.util.Map;

public class DatasetMigrationHandler extends AbstractTestMigrationHandler {

    @Override
    public String getPrefix() {
        return "";
    }

    @Override
    public String getCallbackPropertyName() {
        return "dse_migration_handler_callback";
    }

    @Override
    protected String getDuplication() {
        return "dse_duplication";
    }

    @Override
    protected String getLegacy() {
        return "dse_legacy";
    }

    @Override
    protected String getIncoming() {
        return "dse_incoming";
    }

    @Override
    protected String getOutgoing() {
        return "dse_outgoing";
    }

    @Override
    protected void from(final Map<String, String> incomingData) {
        incomingData.put("dso.dso_from_dataset", "from dataset");
    }

}
