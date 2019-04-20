package org.talend.components.solr.service;

public class TestMessages implements Messages {

    @Override
    public String healthCheckOk() {
        return "OK";
    }

    @Override
    public String healthCheckFailed(String cause) {
        return "FAIL";
    }

    @Override
    public String badCredentials() {
        return "Bad credentials message";
    }

    @Override
    public String unsupportedSolrAction() {
        return "Unsupported Solr Action";
    }
}
