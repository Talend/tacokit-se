package org.talend.components.workday.dataset;

import lombok.Data;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;


@Data
@DataSet("WorkdayDataset")
@GridLayout({ @GridLayout.Row("datastore"), @GridLayout.Row("service") })
@Documentation("")
public class WorkdayDataSet implements Serializable {

    @Option
    @Documentation("The connection to workday datastore")
    private WorkdayDataStore datastore;

    @Option
    @Documentation("A valid read only query is the source type is Query")
    private String service;
}
