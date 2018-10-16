package org.talend.components.localio.devnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.FLOW_TARGET_O;

@Version
@Icon(FLOW_TARGET_O)
@Processor(name = "DevNullOutputRuntime")
@Documentation("This component ignores any input.")
public class DevNullOutputRuntime implements Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final DevNullOutputConfiguration configuration;

    public DevNullOutputRuntime(@Option("configuration") final DevNullOutputConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final Record record) {
        if (configuration.getShouldPrint()) {
            logger.info(record.toString());
        }
    }
}
