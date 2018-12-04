// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jms.output;

import lombok.Data;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout(value = { @GridLayout.Row({ "basicConfig" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "deliveryMode" }) }, names = GridLayout.FormType.ADVANCED)
@Documentation("Main configuration class for JMSOutput component")
@Data
public class OutputConfiguration implements Serializable {

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
