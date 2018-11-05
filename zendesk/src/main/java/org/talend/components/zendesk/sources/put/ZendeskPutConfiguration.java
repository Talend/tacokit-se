package org.talend.components.zendesk.sources.put;

import lombok.Data;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "useBatch" }) })
@Documentation("'Output component' configuration")
public class ZendeskPutConfiguration implements Serializable {

    @Option
    @Documentation("Connection to server")
    private ZendeskDataSet dataSet;

    @Option
    @ActiveIf(target = "dataSet/selectionType", value = { "TICKETS" })
    @Documentation("Using Batching when create and update items. This uses Zendesk jobs. "
            + "Faster but not as reliable as non batch option. " + "Jobs can be aborted or interrupted at any time.")
    private boolean useBatch;

}