package org.talend.components.pubsub.source;

import java.io.Serializable;

import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({ //
        @GridLayout.Row("dataSet"), //
        @GridLayout.Row({ "useMaxReadTime", "maxReadTime" }), //
        @GridLayout.Row({ "useMaxNumRecords", "maxNumRecords" }), //
        @GridLayout.Row("idLabel"), //
        @GridLayout.Row("timestampLabel"), //
})
@Documentation("Configuration for Subscriber")
public class SubscriberMapperConfiguration implements Serializable {

    @Option
    @Documentation("DataSet")
    private PubSubDataSet dataSet;

    @Option
    @Documentation("")
    private Boolean useMaxReadTime = Boolean.FALSE;

    @Option
    @ActiveIf(target = "useMaxReadTime", value = { "true" })
    @Documentation("Max duration (Millions) from start receiving")
    private Long maxReadTime;

    @Option
    @Documentation("")
    private Boolean useMaxNumRecords = Boolean.FALSE;

    @Option
    @ActiveIf(target = "useMaxNumRecords", value = { "true" })
    @Documentation("")
    private Integer maxNumRecords;

    @Option
    @Documentation("")
    private String idLabel;

    @Option
    @Documentation("")
    private String timestampLabel;

}
