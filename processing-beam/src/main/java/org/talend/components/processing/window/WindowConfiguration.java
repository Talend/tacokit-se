package org.talend.components.processing.window;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Documentation("WindowConfiguration, empty for the moment.")
@OptionsOrder({ "windowLength", "windowSlideLength", "windowSession" })
public class WindowConfiguration implements Serializable {

    @Option
    @Required
    @Min(1)
    @Max(Integer.MAX_VALUE)
    @Documentation("The window duration (Data during X ms)")
    private Integer windowLength = 5000;

    @Option
    @Required
    @Min(0)
    @Max(Integer.MAX_VALUE)
    @Documentation("The window slide length (Every X ms)")
    private Integer windowSlideLength = 5000;

    @Option
    @Required
    @Documentation("The window session")
    private Boolean windowSession = false;

}
