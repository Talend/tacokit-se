package org.talend.components.localio.devnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.localio.fixed.FixedDataSetConfiguration;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import lombok.Data;

@Data
@Version
@OptionsOrder({ "dataset", "shouldPrint" })
public class DevNullOutputConfiguration implements Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Option
    @Documentation("")
    private FixedDataSetConfiguration dataset = new FixedDataSetConfiguration();

    @Option
    @Documentation("Display received record on the logs.")
    private Boolean shouldPrint = false;

}
