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
package org.talend.components.jms.source;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout({ @GridLayout.Row({ "durableSubscription" }), @GridLayout.Row({ "clientId", "subscriberName" }) })
@Data
public class DurableSubscriptionConfiguration implements Serializable {

    @Option
    @Documentation("Enable durable subscription")
    private boolean durableSubscription = false;

    @Option
    @Documentation("Client Id")
    @ActiveIf(target = "durableSubscription", value = "true")
    private String clientId;

    @Option
    @Documentation("Subscriber Name")
    @ActiveIf(target = "durableSubscription", value = "true")
    private String subscriberName;

}
