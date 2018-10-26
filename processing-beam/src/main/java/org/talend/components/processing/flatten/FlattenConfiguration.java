package org.talend.components.processing.flatten;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Documentation("Flatten Configuration")
@OptionsOrder({ "columnToFlatten", "isList", "fieldDelimiter", "specificFieldDelimiter", "discardTrailingEmptyStr", "trim" })
public class FlattenConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("")
    private String columnToFlatten = "";

    @Option
    @Required
    @Documentation("")
    private boolean isList = false;

    @Option
    @Required
    @ActiveIf(target = "isList", value = "false")
    @Documentation("")
    private FieldDelimiterType fieldDelimiter = FieldDelimiterType.SEMICOLON;

    @Option
    @Required
    @ActiveIfs({ @ActiveIf(target = "isList", value = "false"), @ActiveIf(target = "fieldDelimiter", value = "OTHER") })
    @Documentation("")
    private String specificFieldDelimiter = FieldDelimiterType.SEMICOLON.getDelimiter();

    @Option
    @Required
    @Documentation("")
    private boolean discardTrailingEmptyStr = false;

    @Option
    @Required
    @Documentation("")
    private boolean trim = false;

    public String getFieldDelimiter() {
        if (FieldDelimiterType.OTHER == fieldDelimiter) {
            return specificFieldDelimiter;
        }
        return fieldDelimiter.getDelimiter();
    }

    @Getter
    @AllArgsConstructor
    public enum FieldDelimiterType {
        SEMICOLON(";"),
        COLON(":"),
        COMMA(","),
        TABULATION("\t"),
        SPACE(" "),
        OTHER("Other");

        private final String delimiter;
    }
}
