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
import lombok.ToString;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@ToString
@GridLayout({ @GridLayout.Row({ "dse" }), @GridLayout.Row({ "sink_legacy" }), @GridLayout.Row({ "sink_duplication" }),
        @GridLayout.Row({ "sink_migration_handler_callback" }), @GridLayout.Row({ "sink_incoming" }),
        @GridLayout.Row({ "sink_outgoing" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {})
public class SinkConfig extends AbstractConfig {

    @Option
    @Documentation("New  property to duplicate sink legacy one.")
    String sink_duplication;

    @Option
    @Documentation("The legacy property outside the dataset for sink.")
    String sink_legacy;

    @Option
    @Documentation("Sink migration handler callback")
    String sink_migration_handler_callback;

    @Option
    @Documentation("Incoming configuration")
    @Code("json")
    String sink_incoming;

    @Option
    @Documentation("Outgoing configuration")
    @Code("json")
    String sink_outgoing;

}
