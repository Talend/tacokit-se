package org.talend.components.salesforce.dataset;

import static org.talend.components.salesforce.dataset.QueryDataSet.SourceType.SOQL_QUERY;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("query")
@GridLayout(value = { @GridLayout.Row("dataStore"), @GridLayout.Row("sourceType"), @GridLayout.Row("query"),
        @GridLayout.Row("moduleName"), @GridLayout.Row("selectColumnIds"), @GridLayout.Row("condition"), })
@Documentation("")
public class QueryDataSet implements Serializable {

    public static Set<String> MODULE_NOT_SUPPORT_BULK_API = new HashSet<String>(Arrays.asList("AcceptedEventRelation",
            "ActivityHistory", "AggregateResult", "AttachedContentDocument", "CaseStatus", "CombinedAttachment", "ContractStatus",
            "DeclinedEventRelation", "EmailStatus", "LookedUpFromActivity", "Name", "NoteAndAttachment", "OpenActivity",
            "OwnedContentDocument", "PartnerRole", "ProcessInstanceHistory", "RecentlyViewed", "SolutionStatus", "TaskPriority",
            "TaskStatus", "UndecidedEventRelation", "UserRecordAccess"));

    @Option
    @Documentation("")
    public BasicDataStore dataStore;

    @Option
    @Required
    @Documentation("")
    public SourceType sourceType = SOQL_QUERY;

    @Option
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Suggestable(value = "loadSalesforceModules", parameters = { "dataStore" })
    @Documentation("")
    public String moduleName;

    @Option
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Documentation("")
    public List<String> selectColumnIds;

    @Option
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Documentation("")
    public String condition;

    @Option
    @ActiveIf(target = "sourceType", value = { "SOQL_QUERY" })
    @Code("sql")
    @Documentation("")
    public String query;

    public enum SourceType {
        MODULE_SELECTION,
        SOQL_QUERY
    }

    private List<String> filter(final List<String> moduleNames) {
        moduleNames.removeAll(MODULE_NOT_SUPPORT_BULK_API);
        return moduleNames;
    }
}
