package org.talend.components.zendesk.sources.delete;

import lombok.Data;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }) })
@Documentation("'Delete component' configuration")
public class ZendeskDeleteConfiguration implements Serializable {

    @Option
    @Documentation("Connection to server")
    private ZendeskDataSet dataSet;

}