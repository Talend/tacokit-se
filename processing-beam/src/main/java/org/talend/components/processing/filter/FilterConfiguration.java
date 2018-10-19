package org.talend.components.processing.filter;

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
@OptionsOrder({ "filters", "logicalOpType" })
@Documentation("A set of filter and a way to combine them to filter data.")
public class FilterConfiguration implements Serializable {

    @Option
    @Documentation("How to combine filters")
    private LogicalOpType logicalOpType = LogicalOpType.ALL;

    @Option
    @Documentation("The list of filters to apply")
    private List<Criteria> filters = Arrays.asList(new Criteria());

    @Data
    @OptionsOrder({ "columnName", "function", "operator", "value" })
    @Documentation("An unitary filter.")
    public static class Criteria implements Serializable {

        @Option
        @Required
        @Suggestable("datalist")
        @Documentation("The column name to use for this criteria")
        private String columnName = "";

        @Option
        @Documentation("The function to apply on the column")
        private ConditionsRowConstant.Function function = ConditionsRowConstant.Function.EMPTY;

        @Option
        @Documentation("The operator")
        private ConditionsRowConstant.Operator operator = ConditionsRowConstant.Operator.EQUAL;

        @Option
        @Required
        @Documentation("The value to compare to")
        private String value = "";
    }
}
