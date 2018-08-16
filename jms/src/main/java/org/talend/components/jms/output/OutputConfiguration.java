package org.talend.components.jms.output;

import lombok.Data;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@DataSet("JMSOutputDataSet")
@GridLayout(value = {
        @GridLayout.Row({"connection"}),
        @GridLayout.Row({"basicConfig"})},
        names = GridLayout.FormType.MAIN)
@GridLayout(value = {
        @GridLayout.Row({"deliveryMode"})},
        names = GridLayout.FormType.ADVANCED)
@Documentation("TODO fill the documentation for this basicConfig")
@Data
public class OutputConfiguration implements Serializable {

    @Option
    @Documentation("JMS connection information")
    private JmsDataStore connection;

    @Option
    @Documentation("Common basicConfig")
    private BasicConfiguration basicConfig;

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
