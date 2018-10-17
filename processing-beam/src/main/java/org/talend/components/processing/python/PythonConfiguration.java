package org.talend.components.processing.python;

import lombok.Data;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.PYTHON;

@Data
@Documentation("Python configuration.")
@OptionsOrder({ "mapType", "pythonCode" })
public class PythonConfiguration implements Serializable {

    public enum MapType {
        MAP,
        FLATMAP
    }

    @Option
    @Required
    @Documentation("The type of processing")
    private MapType mapType = MapType.MAP;

    private static StringBuilder defaultPython = new StringBuilder()
            .append("# Here you can define your custom MAP transformations on the input\n")
            .append("# The input record is available as the \"input\" variable\n")
            .append("# The output record is available as the \"output\" variable\n")
            .append("# The record columns are available as defined in your input/output schema\n")
            .append("# The return statement is added automatically to the generated code,\n")
            .append("# so there's no need to add it here\n\n").append("# Code Sample :\n\n")
            .append("# output['col1'] = input['col1'] + 1234\n").append("# output['col2'] = \"The \" + input['col2'] + \":\"\n")
            .append("# output['col3'] = CustomTransformation(input['col3'])\n");

    @Option
    @Required
    @Code("python")
    @Documentation("The Python code")
    private String pythonCode = defaultPython.toString();
}
