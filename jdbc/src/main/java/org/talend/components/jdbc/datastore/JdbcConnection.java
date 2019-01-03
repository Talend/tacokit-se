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
package org.talend.components.jdbc.datastore;

import lombok.Data;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.components.jdbc.service.UIActionService.ACTION_LIST_HANDLERS_DB;
import static org.talend.components.jdbc.service.UIActionService.ACTION_LIST_SUPPORTED_DB;

@Data
@GridLayout({ @GridLayout.Row({ "dbType", "handler" }), @GridLayout.Row("jdbcUrl"), @GridLayout.Row("userId"),
        @GridLayout.Row("password") })
@DataStore("JdbcConnection")
@Checkable(UIActionService.ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class JdbcConnection implements Serializable {

    @Option
    @Required
    @Documentation("Data base type from the supported data base list")
    @Proposable(ACTION_LIST_SUPPORTED_DB)
    private String dbType;

    @Option
    @ActiveIf(target = "dbType", value = "Aurora")
    @Documentation("Database handlers, this configuration is for cloud databases that support the use of other databases drivers")
    @Suggestable(value = ACTION_LIST_HANDLERS_DB, parameters = { "dbType" })
    private String handler;

    @Option
    @Required
    @Documentation("jdbc connection url")
    private String jdbcUrl;

    @Option
    @Required
    @Documentation("database user")
    private String userId;

    @Option
    @Credential
    @Documentation("database password")
    private String password;

}
