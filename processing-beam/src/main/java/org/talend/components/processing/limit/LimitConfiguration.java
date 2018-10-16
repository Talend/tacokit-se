package org.talend.components.processing.limit;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@Icon(custom = "limit", value = CUSTOM)
@Documentation("Limit configuration.")
@OptionsOrder("limit")
public class LimitConfiguration implements Serializable {

    @Option
    @Min(0)
    @Required
    @Documentation("The limit value (counter)")
    private long limit = 100000000L;

}
