package org.talend.components.zendesk.messages;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface Messages {

    String healthCheckOk();

    String healthCheckFailed(String error);

    String deleteUnsupportedType();

    String UnknownTypeException();

}
