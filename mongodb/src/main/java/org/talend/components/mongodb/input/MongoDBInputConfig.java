package org.talend.components.mongodb.input;

import lombok.Data;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.datastore.MongoDBDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.List;

@Data
@GridLayout(value = { @GridLayout.Row("dataset"),
        @GridLayout.Row({ "setReadPreference", "readPreference" }), @GridLayout.Row("query"), @GridLayout.Row("mapping"),
        @GridLayout.Row("sort"), @GridLayout.Row("limite") })
@Documentation("MongoDBInputConfig")
public class MongoDBInputConfig {

    @Option
    @Documentation("MongoDBDataset")
    private MongoDBDataset dataset;

    @Option
    @Documentation("setReadPreference")
    private boolean setReadPreference;

    @Option
    @ActiveIf(target = "setReadPreference", value = "true")
    @Documentation("readPreference")
    private ReadPreference readPreference;

    public enum ReadPreference {
        Primary,
        Primary_Preferred,
        Secondary,
        Secondary_Preferred,
        Nearest
    }

    @Option
    @TextArea
    @Documentation("query")
    private String query = "{}";

    @Option
    @Documentation("limite")
    private int limite;

    @Option
    @Documentation("mapping")
    private List<InputMapping> mapping;

    @Option
    @Documentation("sort")
    private List<Sort> sort;

}
