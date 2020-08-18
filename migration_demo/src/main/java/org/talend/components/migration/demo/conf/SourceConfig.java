package org.talend.components.migration.demo.conf;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
public class SourceConfig implements Serializable {

    @Option
    @Documentation("")
    private Dataset dse;

    @Option
    @Documentation("")
    private String log_levle;

}
