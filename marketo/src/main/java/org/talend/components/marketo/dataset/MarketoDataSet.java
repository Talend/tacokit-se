// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.dataset;

import java.io.Serializable;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

import static org.talend.components.marketo.service.UIActionService.ACTIVITIES_LIST;
import static org.talend.components.marketo.service.UIActionService.DATE_RANGES;
import static org.talend.components.marketo.service.UIActionService.FIELD_NAMES;
import static org.talend.components.marketo.service.UIActionService.LIST_NAMES;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_LIST_PROPERTY;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_STRING_PROPERTY;

@Data
@DataSet
@Documentation("Marketo Dataset")
@ToString
@GridLayout({ @GridLayout.Row("dataStore"), //
        @GridLayout.Row("leadAction"), //
        @GridLayout.Row("listId"), //
        @GridLayout.Row({ "sinceDateTime", "dateTimeMode" }), //
        @GridLayout.Row({ "activityTypeIds" }), //
        @GridLayout.Row("fields"), //

})
public class MarketoDataSet implements Serializable {

    @Option
    @Documentation("Connection")
    private MarketoDataStore dataStore;

    @Option
    @Documentation("Lead Action")
    private LeadAction leadAction = MarketoDataSet.LeadAction.getLeadsByList;

    @Option
    @Suggestable(value = LIST_NAMES, parameters = { "../dataStore" })
    @Documentation("List")
    private String listId;

    @Option
    @ActiveIf(target = "leadAction", value = { "getLeadChanges", "getLeadActivity" })
    @Documentation("Date Time Mode")
    private DateTimeMode dateTimeMode = DateTimeMode.relative;

    @Option
    @ActiveIf(target = "leadAction", value = { "getLeadChanges", "getLeadActivity" })
    @Suggestable(value = DATE_RANGES, parameters = { "../dateTimeMode" })
    @Validable(VALIDATION_STRING_PROPERTY)
    @Documentation("Since Date Time")
    private String sinceDateTime = String.valueOf(Period.ofWeeks(2).getDays());

    @Option
    @ActiveIf(target = "leadAction", value = "getLeadActivity")
    @Suggestable(value = ACTIVITIES_LIST, parameters = { "../dataStore" })
    @Validable(VALIDATION_LIST_PROPERTY)
    @Documentation("Activity Type Ids (10 max supported")
    private List<String> activityTypeIds = Collections.emptyList();

    @Option
    @ActiveIf(target = "leadAction", negate = true, value = { "getLeadActivity" })
    @Suggestable(value = FIELD_NAMES, parameters = { "../dataStore" })
    @Validable(VALIDATION_LIST_PROPERTY)
    @Documentation("Fields")
    private List<String> fields = Arrays.asList("id", "firstName", "lastName", "email", "createdAt", "updatedAt");

    public enum LeadAction {
        getLeadActivity,
        getLeadChanges,
        getLeadsByList
    }

    public enum DateTimeMode {
        relative,
        absolute
    }
}
