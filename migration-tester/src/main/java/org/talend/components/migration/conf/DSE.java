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
import org.talend.components.migration.migration.DatasetMigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(value = AbstractConfig.VERSION, migrationHandler = DatasetMigrationHandler.class)
@GridLayout({ @GridLayout.Row({ "dso" }), @GridLayout.Row({ "dse_legacy" }), @GridLayout.Row({ "dse_duplication" }),
        @GridLayout.Row({ "dse_migration_handler_callback" }), @GridLayout.Row({ "dse_incoming" }),
        @GridLayout.Row({ "dse_outgoing" }), @GridLayout.Row({ "dse_from_source" }), @GridLayout.Row({ "dse_from_sink" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {})
@Data
@DataSet("Dataset")
public class DSE implements Serializable {

    @Option
    @Documentation("The datastore")
    DSO dso;

    @Option
    @Documentation("New  property to duplicate legacy one.")
    String dse_duplication;

    @Option
    @Documentation("The legacy property of the dataset.")
    String dse_legacy;

    @Option
    @Documentation("Dataset migration handler callback")
    String dse_migration_handler_callback;

    @Option
    @Documentation("Incoming configuration")
    @Code("json")
    String dse_incoming;

    @Option
    @Documentation("Outgoing configuration")
    @Code("json")
    String dse_outgoing;

    @Option
    @Documentation("Property updated from source migration handler")
    String dse_from_source;

    @Option
    @Documentation("Property updated from sink migration handler")
    String dse_from_sink;

}
