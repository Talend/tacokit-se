package org.talend.components.zendesk.common;

import lombok.Data;
import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataSet(ConfigurationHelper.DATA_SET_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "selectionType" }) })
@Documentation("'Get component' configuration")
public class ZendeskDataSet implements Serializable {

    @Option
    @Documentation("Connection to server")
    private ZendeskDataStore dataStore;

    @Option
    @Documentation("The type of information we want to get, e.g. 'Requests'")
    private SelectionType selectionType = SelectionType.TICKETS;

}