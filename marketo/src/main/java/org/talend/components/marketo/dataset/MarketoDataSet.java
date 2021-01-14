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
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

import static org.talend.components.marketo.dataset.MarketoDataSet.DateTimeRelative.PERIOD_AGO_2W;
import static org.talend.components.marketo.service.UIActionService.ACTIVITIES_LIST;
import static org.talend.components.marketo.service.UIActionService.FIELD_NAMES;
import static org.talend.components.marketo.service.UIActionService.LIST_NAMES;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_DATETIME_PATTERN;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_LIST_PROPERTY;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.AND;

@Data
@DataSet
@Documentation("Marketo Dataset")
@ToString
@GridLayout({ @GridLayout.Row("dataStore"), //
        @GridLayout.Row("leadAction"), //
        @GridLayout.Row("listId"), //
        @GridLayout.Row({ "dateTimeMode", "sinceDateTimeRelative", "sinceDateTimeAbsolute" }), //
        @GridLayout.Row({ "activityTypeIds" }), //
        @GridLayout.Row("fields"), //

})
public class MarketoDataSet implements Serializable {

    @Option
    @Documentation("Connection")
    private MarketoDataStore dataStore;

    @Option
    @Documentation("Lead action")
    private LeadAction leadAction = MarketoDataSet.LeadAction.getLeadsByList;

    @Option
    @Suggestable(value = LIST_NAMES, parameters = { "../dataStore" })
    @Documentation("List")
    private String listId;

    @Option
    @ActiveIf(target = "leadAction", value = { "getLeadActivity" })
    @Documentation("Date time mode")
    private DateTimeMode dateTimeMode = DateTimeMode.relative;

    @Option
    @ActiveIfs(operator = AND, value = { //
            @ActiveIf(target = "leadAction", value = { "getLeadActivity" }), //
            @ActiveIf(target = "dateTimeMode", value = { "relative" }) })
    @Documentation("Since relative date time")
    private DateTimeRelative sinceDateTimeRelative = PERIOD_AGO_2W;

    @Option
    @ActiveIfs(operator = AND, value = { //
            @ActiveIf(target = "leadAction", value = { "getLeadActivity" }), //
            @ActiveIf(target = "dateTimeMode", value = { "absolute" }) })
    @Validable(VALIDATION_DATETIME_PATTERN)
    @Documentation("Since absolute date time")
    private String sinceDateTimeAbsolute;

    @Option
    @ActiveIf(target = "leadAction", value = "getLeadActivity")
    @Suggestable(value = ACTIVITIES_LIST, parameters = { "../dataStore" })
    @Validable(VALIDATION_LIST_PROPERTY)
    @Documentation("Activity type ids (10 max supported)")
    private List<String> activityTypeIds = Collections.emptyList();

    @Option
    @ActiveIf(target = "leadAction", negate = true, value = { "getLeadActivity" })
    @Suggestable(value = FIELD_NAMES, parameters = { "../dataStore" })
    @Documentation("Fields")
    private List<String> fields = Collections.emptyList();

    public enum LeadAction {
        getLeadActivity,
        getLeadsByList
    }

    public enum DateTimeMode {
        relative,
        absolute
    }

    public enum DateTimeRelative {
        PERIOD_AGO_1W("P7D"), //
        PERIOD_AGO_2W("P14D"), //
        PERIOD_AGO_1M("P1M"), //
        PERIOD_AGO_3M("P3M"), //
        PERIOD_AGO_6M("P6M"), //
        PERIOD_AGO_1Y("P1Y"), //
        PERIOD_AGO_2Y("P2Y"); //

        @lombok.Getter
        private String relativeOffset;

        DateTimeRelative(String value) {
            this.relativeOffset = value;
        }
    }

}
