package org.talend.components.processing.normalize;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.NORMALIZE;

@Data
@Icon(NORMALIZE)
@Documentation("Normalization configuration.")
@OptionsOrder({ "columnToNormalize", "fieldSeparator", "otherSeparator", "discardTrailingEmptyStr", "trim" })
public class NormalizeConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("The column to normalize")
    private String columnToNormalize = "";

    @Option
    @Documentation("Should discard trailing spaces")
    private boolean discardTrailingEmptyStr;

    @Option
    @Documentation("Should trim")
    private boolean trim;

    @Option
    @ActiveIf(target = "isList", value = "false")
    @Documentation("Is the column a list")
    private Delimiter fieldSeparator = Delimiter.SEMICOLON;

    @Option
    @ActiveIf(target = "isList", value = "false")
    @ActiveIf(target = "fieldSeparator", value = "OTHER")
    @Documentation("Is the column a list")
    private String otherSeparator = "";

    @Getter
    @AllArgsConstructor
    public enum Delimiter {
        SEMICOLON(";"),
        COLON(":"),
        COMMA(","),
        TABULATION("\t"),
        SPACE(" "),
        OTHER("");

        private final String delimiter;
    }
}
