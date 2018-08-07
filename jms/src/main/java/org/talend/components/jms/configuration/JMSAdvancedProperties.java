package org.talend.components.jms.configuration;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

@Data
public class JMSAdvancedProperties {

    @Option
    @Documentation("Key column")
    private String key;

    @Option
    @Documentation("Value column")
    private String value;
}
