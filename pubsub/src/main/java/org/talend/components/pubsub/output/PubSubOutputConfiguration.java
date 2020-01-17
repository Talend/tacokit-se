package org.talend.components.pubsub.output;

import lombok.Data;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "pubsub")
@Documentation("Pub/Sub output component configuration.")
@OptionsOrder({ "dataSet", "tableOperation" })
public class PubSubOutputConfiguration implements Serializable {

    @Option
    @Documentation("PubSub dataset")
    private PubSubDataSet dataset;

    @Option
    @DefaultValue("CREATE_IF_NOT_EXISTS")
    @Documentation("Topic operation")
    public TopicOperation topicOperation;

    public enum TopicOperation {
        NONE,
        CREATE_IF_NOT_EXISTS,
        DROP_IF_EXISTS_AND_CREATE,
    }
}
