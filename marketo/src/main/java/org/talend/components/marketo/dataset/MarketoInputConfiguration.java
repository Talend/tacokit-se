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

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

import static org.talend.components.marketo.service.UIActionService.LEAD_KEY_NAME_LIST;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_INTEGER_PROPERTY;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_STRING_PROPERTY;

@Data
@GridLayout({ //
        @GridLayout.Row({ "dataSet" }), //
        @GridLayout.Row({ "leadKeyName" }), //
        @GridLayout.Row({ "leadKeyValues" }), //
        @GridLayout.Row({ "leadId", }), //
        @GridLayout.Row({ "leadIds" }), //
        @GridLayout.Row({ "assetIds" }), //
}) //
@Documentation("Marketo Source Configuration")
@ToString(callSuper = true)
public class MarketoInputConfiguration implements Serializable {

    public static final String NAME = "MarketoInputConfiguration";

    /*
     * DataSet
     */
    @Option
    @Documentation("Marketo DataSet")
    private MarketoDataSet dataSet;

    @Option
    @ActiveIf(target = "leadAction", value = "getLead")
    @Min(0)
    @Validable(VALIDATION_INTEGER_PROPERTY)
    @Documentation("Lead Id")
    private Integer leadId;

    @Option
    @ActiveIf(target = "leadAction", value = "getMultipleLeads")
    @Suggestable(value = LEAD_KEY_NAME_LIST, parameters = { "../dataSet/dataStore" })
    @Validable(VALIDATION_STRING_PROPERTY)
    @Documentation("Key Name")
    private String leadKeyName;

    @Option
    @ActiveIf(target = "leadAction", value = "getMultipleLeads")
    @Validable(VALIDATION_STRING_PROPERTY)
    @Documentation("Values (Comma-separated)")
    private String leadKeyValues;

    /*
     * Changes & Activities
     */
    @Option
    @ActiveIf(target = "leadAction", value = { "getLeadChanges", "getLeadActivity" })
    @Documentation("Lead Ids (Comma-separated Lead Ids)")
    private String leadIds;

    @Option
    @ActiveIf(target = "leadAction", value = { "getLeadActivity" })
    @Documentation("Asset Ids (Comma-separated Asset Ids)")
    private String assetIds;

    public enum ListAction {
        list,
        get,
        isMemberOf,
        getLeads
    }

    public enum OtherEntityAction {
        list,
        get
    }

}
