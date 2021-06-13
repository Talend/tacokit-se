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
package org.talend.components.zendesk.output;

import java.io.Serializable;

import org.talend.components.zendesk.common.ZendeskDataSet;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "dataAction" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "useBatch" }) })
@Documentation("'Output component' configuration.")
public class ZendeskOutputConfiguration implements Serializable {

    @Option
    @Documentation("Connection to server.")
    private ZendeskDataSet dataset;

    @Option
    @ActiveIf(target = "dataset/selectionType", value = { "TICKETS" })
    @Documentation("Using Batching when create and update items. This uses Zendesk jobs. "
            + "Faster but not as reliable as non batch option. " + "Jobs can be aborted or interrupted at any time.")
    private boolean useBatch;

    @Option
    @Documentation("Data Action for output.")
    private DataAction dataAction = DataAction.CREATE;
}