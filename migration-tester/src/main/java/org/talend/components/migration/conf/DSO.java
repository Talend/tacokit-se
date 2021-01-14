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
package org.talend.components.migration.conf;

import lombok.Data;
import org.talend.components.migration.migration.DatastoreMigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(value = AbstractConfig.VERSION, migrationHandler = DatastoreMigrationHandler.class)
@GridLayout({ @GridLayout.Row({ "dso_legacy" }), @GridLayout.Row({ "dso_duplication" }),
        @GridLayout.Row({ "dso_migration_handler_callback" }), @GridLayout.Row({ "dso_incoming" }),
        @GridLayout.Row({ "dso_outgoing" }), @GridLayout.Row({ "dso_from_dataset" }), @GridLayout.Row({ "dso_from_source" }),
        @GridLayout.Row({ "dso_from_sink" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dso_shouldNotBeEmpty" }) })
@Data
@DataStore("Datastore")
public class DSO implements Serializable {

    @Option
    @Documentation("New  property to duplicate legacy one.")
    String dso_duplication;

    @Option
    @Documentation("The legacy property of the datastore.")
    String dso_legacy;

    @Option
    @Documentation("Datastore migration handler callback")
    String dso_migration_handler_callback;

    @Option
    @Documentation("Incoming configuration")
    @Code("json")
    String dso_incoming;

    @Option
    @Documentation("Outgoing configuration")
    @Code("json")
    String dso_outgoing;

    @Option
    @Documentation("Property updated from dataset migration handler")
    String dso_from_dataset;

    @Option
    @Documentation("Property updated from source migration handler")
    String dso_from_source;

    @Option
    @Documentation("Property updated from sink migration handler")
    String dso_from_sink;

    @Option
    @Documentation("New property that add a breaking change with v100. If empty the source/sink connectors will generate an exception.")
    @Required
    String dso_shouldNotBeEmpty;

}
