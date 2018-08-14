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
