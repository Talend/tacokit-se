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
package org.talend.components.dynamicscrm.source;

import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

import java.io.Serializable;
import java.util.List;

import org.talend.components.dynamicscrm.dataset.DynamicsCrmDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "customFilter" }), @GridLayout.Row({ "operator" }),
        @GridLayout.Row({ "filterConditions" }), @GridLayout.Row({ "filter" }), @GridLayout.Row({ "orderByConditionsList" }) })
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("dataset") })
@Documentation("Dynamics CRM input configuration")
public class DynamicsCrmInputMapperConfiguration implements Serializable {

    @Option
    @Documentation("Dynamics CRM dataset")
    private DynamicsCrmDataset dataset;

    @Option
    @Documentation("Use advanced filter string instead of filter conditions table")
    private boolean customFilter = false;

    @Option
    @ActiveIf(target = "customFilter", value = "false")
    @Documentation("Logical operator used to combine conditions")
    private Operator operator = Operator.AND;

    @Option
    @ActiveIf(target = "customFilter", value = "false")
    @Documentation("Filter conditions")
    private List<FilterCondition> filterConditions;

    @Option
    @ActiveIf(target = "customFilter", value = "true")
    @Documentation("Filter")
    private String filter;

    @Option
    @Documentation("Sorting conditions")
    private List<OrderByCondition> orderByConditionsList;

    @Option
    // @Structure(type = Type.OUT)
    @Documentation("Fields to get from CRM")
    private List<String> columns;

    public enum Operator {
        AND,
        OR;
    }

}