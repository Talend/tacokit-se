package org.talend.components.zendesk.sources.get;

import lombok.Data;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "queryString" }), @GridLayout.Row({ "fields" }) })
@Documentation("'Input component' configuration")
public class ZendeskGetConfiguration implements Serializable {

    @Option
    @Documentation("Connection to server")
    private ZendeskDataSet dataSet;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

    @Option
    @ActiveIf(target = "dataSet/selectionType", value = { "TICKETS" })
    @Documentation("Query string. See Zendesk API documentation. Example: 'status:open created>2012-07-17'")
    private String queryString;

    public boolean isQueryStringEmpty() {
        return queryString == null || queryString.isEmpty();
    }

}