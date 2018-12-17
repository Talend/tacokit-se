package org.talend.components.pubsub.output;

import java.io.Serializable;

import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({ //
        @GridLayout.Row("dataSet"), //
        @GridLayout.Row("topicOperation"), //
        @GridLayout.Row("idLabel"), //
        @GridLayout.Row("timestampLabel"), //
})
@Documentation("Configuration for Publisher")
public class PublisherOutputConfiguration implements Serializable {

    @Option
    private PubSubDataSet dataSet;

    @Option
    @Documentation("Operation on Topic")
    public TopicOperation topicOperation;

    @Option
    @Documentation("Id Label")
    public String idLabel;

    @Option
    @Documentation("")
    public String timestampLabel;

    public enum TopicOperation {
        NONE,
        CREATE_IF_NOT_EXISTS,
        DROP_IF_EXISTS_AND_CREATE,
    }

}
