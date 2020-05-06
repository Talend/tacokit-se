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
package org.talend.components.rabbitmq.datastore;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.components.rabbitmq.service.ActionService.ACTION_BASIC_HEALTH_CHECK;

@Data
@GridLayout({ @GridLayout.Row({ "hostname", "port" }), @GridLayout.Row({ "userName", "password" }), @GridLayout.Row({ "TLS" }) })
@DataStore("basic")
@Checkable(ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class RabbitMQDataStore implements Serializable {

    @Option
    @Documentation("RabbitMQ server hostname")
    private String hostname;

    @Option
    @Documentation("RabbitMQ server port")
    private Integer port = 5672;

    @Option
    @Documentation("User Name")
    private String userName;

    @Option
    @Credential
    @Documentation("Password")
    private String password;

    @Option
    @Documentation("TLS mode")
    private Boolean TLS = false;
}
