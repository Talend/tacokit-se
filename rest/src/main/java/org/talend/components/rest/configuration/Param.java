package org.talend.components.rest.configuration;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@AllArgsConstructor
@NoArgsConstructor
@GridLayout({ @GridLayout.Row({ "key", "value" }), })
@Documentation("")
public class Param implements Serializable {

    @Option
    @Required
    @Documentation("")
    private String key;

    @Option
    @Required
    @Documentation("")
    private String value;

}
