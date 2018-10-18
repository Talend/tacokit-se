package org.talend.components.processing.fieldselector;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Data
@OptionsOrder("selectors")
@Documentation("Create an output with only specific selected fields.")
public class FieldSelectorConfiguration implements Serializable {

    @Option
    @Documentation("The list of columns to extract to create the new record.")
    private List<Selector> selectors = Arrays.asList(new Selector());

    @Data
    @OptionsOrder({ "field", "path" })
    @Documentation("Identifies a field.")
    public static class Selector implements Serializable {

        @Option
        @Required
        @Documentation("The target field name")
        private String field = "";

        @Option
        @Required
        @Suggestable("datalist")
        @Documentation("The extraction path")
        private String path = "";

    }
}
