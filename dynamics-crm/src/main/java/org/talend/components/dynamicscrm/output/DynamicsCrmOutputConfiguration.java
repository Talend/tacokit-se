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
package org.talend.components.dynamicscrm.output;

import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

import java.io.Serializable;
import java.util.List;

import org.talend.components.dynamicscrm.dataset.DynamicsCrmDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.Structure.Type;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "action" }), @GridLayout.Row({ "lookupMapping" }) })
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("dataset"), @GridLayout.Row("emptyStringToNull"),
        @GridLayout.Row("ignoreNull") })
@Documentation("Azure Dynamics 365 output configuration")
public class DynamicsCrmOutputConfiguration implements Serializable {

    @Option
    @Documentation("Azure Dynamics 365 dataset")
    private DynamicsCrmDataset dataset;

    @Option
    @Documentation("Lookup fields mapping to entity set")
    @ActiveIf(target = "action", value = "DELETE", negate = true)
    private List<LookupMapping> lookupMapping;

    @Option
    @Required
    @Documentation("Action to be performed on data")
    private Action action = Action.INSERT;

    @Option
    @Required
    @Documentation("Ignore null objects for insert and update operations")
    private boolean ignoreNull;

    @Option
    @Required
    @Documentation("Convert empty strings to null for lookup properties")
    private boolean emptyStringToNull;

    @Option
    // @Structure(type = Type.IN)
    @Documentation("Fields to write to CRM")
    private List<String> columns;

    public enum Action {
        INSERT,
        UPSERT,
        DELETE;
    }
}