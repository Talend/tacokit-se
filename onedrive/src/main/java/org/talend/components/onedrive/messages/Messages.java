package org.talend.components.onedrive.messages;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface Messages {

    String healthCheckOk();

    String healthCheckFailed(String error);

    String healthCheckLoginIsEmpty();

    String healthCheckTenantIdIsEmpty();

    String healthCheckApplicationIdIsEmpty();

}
