package org.talend.components.mongodb;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface Messages {

    String healthCheckOk();

    String healthCheckFailed(final String cause);

    String UnsupportedOperation();

}
