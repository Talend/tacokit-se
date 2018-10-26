package org.talend.components.processing.window;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Documentation("WindowConfiguration, empty for the moment.")
@OptionsOrder({ "windowSession", "windowLength", "windowSlideLength" })
public class WindowConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Enable the Window session to set a gap between each window (the gap starts at the latest arrived record)")
    private Boolean windowSession = false;

    @Option
    @Required
    @Min(1)
    @Max(Integer.MAX_VALUE)
    @Documentation("Set the Window duration (session disabled), or set the gap after the latest arrived record (session enabled)")
    private Integer windowLength = 5000;

    @Option
    @Required
    @ActiveIf(target = "windowSession", value = "false")
    @Min(0)
    @Max(Integer.MAX_VALUE)
    @Documentation("The sliding duration between the previous Window beginning and the next Window (ms)")
    private Integer windowSlideLength = 5000;

}
