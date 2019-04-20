package org.talend.components.solr.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface Messages {

    String healthCheckOk();

    String healthCheckFailed(final String cause);

    String badCredentials();

    String unsupportedSolrAction();
}
