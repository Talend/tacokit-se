package org.talend.components.jms.output;

import lombok.Data;
import org.talend.components.jms.configuration.Configuration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout(value = {
        @GridLayout.Row({"moduleList"}),
        @GridLayout.Row({"url"}),
        @GridLayout.Row({"userIdentity"}),
        @GridLayout.Row({"userName", "password"}),
        @GridLayout.Row({"userJNDILookup"}),
        @GridLayout.Row({"messageType"}),
        @GridLayout.Row({"destination"}),
        @GridLayout.Row({"processingMode"})},
        names = GridLayout.FormType.MAIN)
@GridLayout(value = {
        @GridLayout.Row({"deliveryMode"}),
        @GridLayout.Row({"properties"})},
        names = GridLayout.FormType.ADVANCED)
@Documentation("TODO fill the documentation for this configuration")
@Data
public class OutputConfiguration extends Configuration {

    @Option
    @Documentation("Drop down list for Delivery Mode")
    private DeliveryMode deliveryMode = DeliveryMode.NOT_PERSISTENT;

    public static enum DeliveryMode {
        NOT_PERSISTENT(1),
        PERSISTENT(2);

        private int intValue;

        DeliveryMode(int value) {
            this.intValue = value;
        }

        public int getIntValue() {
            return intValue;
        }
    }

}
