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

import static org.talend.components.marketo.service.UIActionService.CUSTOM_OBJECT_NAMES;
import static org.talend.components.marketo.service.UIActionService.FIELD_NAMES;
import static org.talend.components.marketo.service.UIActionService.GUESS_ENTITY_SCHEMA_OUTPUT;

import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.Structure.Type;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

@Data
@DataSet(MarketoOutputDataSet.NAME)
@GridLayouts({ //
        @GridLayout({ //
                @GridLayout.Row({ "dataStore" }), //
                @GridLayout.Row({ "entity" }), //
                @GridLayout.Row({ "action" }), //
                @GridLayout.Row({ "listAction" }), //
                @GridLayout.Row({ "syncMethod" }), //
                @GridLayout.Row({ "lookupField" }), //
                @GridLayout.Row({ "dedupeBy" }), //
                @GridLayout.Row({ "deleteBy" }), //
                @GridLayout.Row({ "customObjectName" }), //
                @GridLayout.Row({ "batchSize" }) //
        }), //
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "schema" }),
                @GridLayout.Row({ "rejectSchema" }), //
        })//
})
@Documentation("Marketo Processor DataSet")
@ToString(callSuper = true)
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
    @Structure(discoverSchema = GUESS_ENTITY_SCHEMA_OUTPUT, type = Type.OUT)
    @Documentation("Flow Schema")
    private List<String> schema;

    @Option
    @Structure(value = "Reject", discoverSchema = GUESS_ENTITY_SCHEMA_OUTPUT, type = Type.OUT)
    @Documentation("Reject Schema")
    private List<String> rejectSchema;

    @Option
    @ActiveIf(target = "entity", value = { "Lead", "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @Documentation("Action")
    private OutputAction action;

    /*
     * Lead Entity
     */
    @Option
    @ActiveIf(target = "entity", value = "Lead")
    @ActiveIf(target = "action", value = "sync")
    @Suggestable(value = FIELD_NAMES, parameters = { "dataStore", "entity", "customObjectName" })
    @Documentation("Lookup Field")
    private String lookupField;

    /*
     * CustomObject Entity
     */
    @Option
    @ActiveIf(target = "entity", value = { "CustomObject" })
    @Suggestable(value = CUSTOM_OBJECT_NAMES, parameters = { "dataStore" })
    @Documentation("Custom Object Name")
    private String customObjectName;

    /*
     * List Entity
     */
    @Option
    @ActiveIf(target = "entity", value = { "List" })
    @Documentation("List Action")
    private ListAction listAction;

    /*
     * All entities
     */
    @Option
    @ActiveIf(target = "entity", value = { "Lead", "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @ActiveIf(target = "action", value = { "sync" })
    @Documentation("Synchronization method")
    private SyncMethod syncMethod;

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
