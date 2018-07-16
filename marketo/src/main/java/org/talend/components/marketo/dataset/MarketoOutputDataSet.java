// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

import lombok.Data;

import org.apache.avro.Schema;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(MarketoOutputDataSet.NAME)
@GridLayout({ //
        @GridLayout.Row({ "dataStore" }), //
        @GridLayout.Row({ "entity", "action", "listAction", "syncMethod", "lookupField", "dedupeBy", "deleteBy" }), //
        @GridLayout.Row({ "batchSize" }) })
@Documentation("Marketo Processor DataSet")
public class MarketoOutputDataSet extends MarketoDataSet {

    public static final String NAME = "MarketoOutputDataSet";

    public enum OutputAction {
        sync,
        delete
    }

    public enum ListAction {
        addTo,
        isMemberOf,
        removeFrom
    }

    public enum SyncMethod {
        createOnly,
        updateOnly,
        createOrUpdate,
        createDuplicate
    }

    public enum DeleteBy {
        dedupeFields,
        idField
    }

    @Option
    @Structure
    @Documentation("Flow Schema")
    private Schema flowSchema;

    @Option
    @Structure
    @Documentation("Reject Schema")
    private Schema rejectSchema;

    @Option
    @ActiveIf(target = "entity", value = { "Lead", "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @Documentation("Action")
    private OutputAction action;

    /*
     * List Entity
     */

    @Option
    @ActiveIf(target = "entity", value = { "List" })
    @Documentation("List Action")
    private ListAction listAction;

    @Option
    @ActiveIf(target = "entity", value = { "Lead", "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @Documentation("Synchronization method")
    private SyncMethod syncMethod;

    @Option
    @ActiveIf(target = "entity", value = "Lead")
    @ActiveIf(target = "action", value = "sync")
    @Documentation("Lookup Field")
    private String lookupField;

    @Option
    @ActiveIf(target = "action", value = { "sync" })
    @ActiveIf(target = "syncMethod", value = { "updateOnly" })
    @Documentation("Dedupe by")
    private String dedupeBy;

    @Option
    @ActiveIf(target = "entity", value = { "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @ActiveIf(target = "action", value = { "delete" })
    @Documentation("Field to delete company records by. Key may be dedupeFields or idField")
    private DeleteBy deleteBy;

}
