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
package org.talend.components.jdbc.datastore;

import lombok.Data;
import lombok.ToString;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.action.Updatable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static org.talend.components.jdbc.service.UIActionService.ACTION_LIST_HANDLERS_DB;
import static org.talend.components.jdbc.service.UIActionService.ACTION_LIST_SUPPORTED_DB;

@Data
@ToString(exclude = { "password" })
@GridLayout({ @GridLayout.Row({ "dbType", "handler" }), @GridLayout.Row("explodedURL"), @GridLayout.Row("userId"),
        @GridLayout.Row("password") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "defineProtocol", "protocol" }),
        @GridLayout.Row("connectionTimeOut"), @GridLayout.Row("connectionValidationTimeOut") })
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
    @ActiveIf(target = "dbType", value = { "Aurora", "SingleStore" })
    @Documentation("Database handlers, this configuration is for cloud databases that support the use of other databases drivers")
    @Suggestable(value = ACTION_LIST_HANDLERS_DB, parameters = { "dbType" })
    private String handler;

    @Option
    @Documentation("Exploded jdbc url")
    @Updatable(value = "DEFAULT_URL", parameters = { "dbType", "handler", "explodedURL" }, after = "defineUrl")
    private ExplodedURL explodedURL;

    @Option
    @Required
    @Documentation("database user")
    private String userId;

    @Option
    @Documentation("Let user define protocol of the jdbc url.")
    @DefaultValue("false")
    private Boolean defineProtocol = false;

    @Option
    @ActiveIf(target = "defineProtocol", value = { "true" })
    @Documentation("Protocol")
    private String protocol;

    @Option
    @Credential
    @Documentation("database password")
    private String password;

    @Min(0)
    @Option
    @Documentation("Set the maximum number of seconds that a client will wait for a connection from the pool. "
            + "If this time is exceeded without a connection becoming available, a SQLException will be thrown from DataSource.getConnection().")
    private long connectionTimeOut = 30;

    @Min(0)
    @Option
    @Documentation("Sets the maximum number of seconds that the pool will wait for a connection to be validated as alive.")
    private long connectionValidationTimeOut = 10;

    @Data
    @GridLayout({ @GridLayout.Row("defineUrl"), @GridLayout.Row("jdbcUrl"), @GridLayout.Row("host"), @GridLayout.Row("port"),
            @GridLayout.Row("database"), @GridLayout.Row("parameters") })
    @Documentation("Exploded jdbc url")
    public static class ExplodedURL implements Serializable {

        @Option
        @Documentation("Let user define complet jdbc url or not")
        @DefaultValue("false")
        private Boolean defineUrl = false;

        @Option
        @ActiveIf(target = "defineUrl", value = { "true" })
        @Documentation("jdbc connection url")
        private String jdbcUrl;

        @Option
        @ActiveIf(target = "defineUrl", value = { "false" })
        @Documentation("jdbc host")
        private String host;

        @Option
        @ActiveIf(target = "defineUrl", value = { "false" })
        @Documentation("jdbc port")
        private int port;

        @Option
        @ActiveIf(target = "defineUrl", value = { "false" })
        @Documentation("jdbc database")
        private String database;

        @Option
        @ActiveIf(target = "defineUrl", value = { "false" })
        @Documentation("jdbc parameters")
        private List<JdbcConfiguration.KeyVal> parameters;
    }

}
