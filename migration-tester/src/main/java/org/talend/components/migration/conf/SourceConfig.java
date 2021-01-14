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
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayout({ @GridLayout.Row({ "dse" }), @GridLayout.Row({ "source_legacy" }), @GridLayout.Row({ "source_duplication" }),
        @GridLayout.Row({ "source_migration_handler_callback" }), @GridLayout.Row({ "source_incoming" }),
        @GridLayout.Row({ "source_outgoing" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {})
public class SourceConfig extends AbstractConfig {

    @Option
    @Documentation("New  property to duplicate legacy one.")
    String source_duplication;

    @Option
    @Documentation("The legacy property outside the dataset.")
    String source_legacy;

    @Option
    @Documentation("Source migration handler callback")
    String source_migration_handler_callback;

    @Option
    @Documentation("Incoming configuration")
    @Code("json")
    String source_incoming;

    @Option
    @Documentation("Outgoing configuration")
    @Code("json")
    String source_outgoing;
}
