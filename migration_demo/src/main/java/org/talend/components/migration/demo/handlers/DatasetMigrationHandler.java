package org.talend.components.migration.demo.handlers;

import org.talend.sdk.component.api.component.MigrationHandler;

import java.util.Map;

public class DatasetMigrationHandler implements MigrationHandler {
    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        return null;
    }
}
