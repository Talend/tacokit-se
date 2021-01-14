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
package org.talend.components.marketo.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

import static org.talend.components.marketo.service.UIActionService.LEAD_KEY_NAME_LIST;

@Data
@GridLayout({ //
        @GridLayout.Row({ "dataSet" }), //
        @GridLayout.Row({ "action" }), //
        @GridLayout.Row({ "lookupField" }), //
}) //
@Documentation("Marketo Sink Configuration")
@ToString(callSuper = true)
public class MarketoOutputConfiguration implements Serializable {

    public static final String NAME = "MarketoOutputConfiguration";

    public enum OutputAction {
        createOnly,
        updateOnly,
        createOrUpdate,
        createDuplicate,
        delete
    }

    /*
     * DataSet
     */
    @Option
    @Documentation("Marketo DataSet")
    private MarketoDataSet dataSet;

    @Option
    @Documentation("Action")
    private OutputAction action = OutputAction.createOrUpdate;

    /*
     * Lead Entity
     */
    @Option
    @ActiveIf(negate = true, target = "action", value = "delete")
    @Suggestable(value = LEAD_KEY_NAME_LIST, parameters = { "../dataSet/dataStore" })
    @Documentation("Lookup Field")
    private String lookupField;

}
