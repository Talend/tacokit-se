package org.talend.components.rest.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18n {

    String healthCheckOk();

    String healthCheckFailed(final String URL);

    String malformedURL(final String URL, final String cause);

    String badRequestBody(final String type);

}
