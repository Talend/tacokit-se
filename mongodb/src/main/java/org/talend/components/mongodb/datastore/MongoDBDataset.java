package org.talend.components.mongodb.datastore;

import lombok.Data;
import org.talend.components.mongodb.input.InputMapping;
import org.talend.components.mongodb.input.Sort;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.List;

@Data
@DataSet("MongoDBDataset")
@GridLayout(value = { @GridLayout.Row("dataStore"), @GridLayout.Row("collection"), @GridLayout.Row("schema") })
@Documentation("MongoDBInputConfig")
public class MongoDBDataset {

    @Option
    @Documentation("datastore")
    private MongoDBDataStore dataStore;

    @Option
    @Documentation("collection")
    private String collection;

    @Option
    @Structure
    @Documentation("schema")
    private List<String> schema;

}
