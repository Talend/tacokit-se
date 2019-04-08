package org.talend.components.rest.configuration;

import java.io.Serializable;

import lombok.NonNull;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "key", "value" }), })
@Documentation("")
public class Param implements Serializable {

    @Option
    @NonNull
    @Documentation("")
    private String key;

    @Option
    @NonNull
    @Documentation("")
    private String value;

}
