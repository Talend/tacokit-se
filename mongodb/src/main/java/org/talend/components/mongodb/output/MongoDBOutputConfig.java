package org.talend.components.mongodb.output;

import lombok.Data;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.datastore.MongoDBDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.List;

@Data
@DataSet("Output")
@GridLayout(value = { @GridLayout.Row("dataset"),
        @GridLayout.Row({ "setWriteConcern", "writeConcern" }), @GridLayout.Row({ "setBuckWrite", "bulkType" }),
        @GridLayout.Row("mapping"), @GridLayout.Row("bulkWriteSize") })
@Documentation("MongoDBOutputConfig")
public class MongoDBOutputConfig {

    @Option
    @Documentation("MongoDBDataset")
    private MongoDBDataset dataset;

    @Option
    @Documentation("setWriteConcern")
    private boolean setWriteConcern;

    @Option
    @Documentation("writeConcern")
    @ActiveIf(target = "setWriteConcern", value = "true")
    private WriteConcern writeConcern;

    private enum WriteConcern {
        ACKNOWLEDGED,
        UNACKNOWLEDGED,
        JOURNALED,
        REPLICA_ACKNOWLEDGED
    }

    @Option
    @Documentation("setBuckWrite")
    private boolean setBuckWrite;

    @Option
    @Documentation("bulkType")
    @ActiveIf(target = "setBuckWrite", value = "true")
    private BulkType bulkType;

    public enum BulkType {
        ORDERED,
        UNORDERED
    }

    @Option
    @Documentation("bulkWriteSize")
    @ActiveIf(target = "setBuckWrite", value = "true")
    private int bulkWriteSize = 1000;

    @Option
    @Documentation("mapping")
    private List<OutputMapping> mapping;

}
