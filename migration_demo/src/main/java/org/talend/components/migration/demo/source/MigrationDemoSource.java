package org.talend.components.migration.demo.source;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.migration.demo.conf.Datastore;
import org.talend.components.migration.demo.conf.SourceConfig;
import org.talend.components.migration.demo.handlers.SourceMigrationHandler;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Slf4j
@Version(value = Datastore.VERSION, migrationHandler = SourceMigrationHandler.class)
@Icon(Icon.IconType.STAR)
@org.talend.sdk.component.api.input.Emitter(name = "MigrationDemoSource")
@Documentation("")
public class MigrationDemoSource implements Serializable {

    private final SourceConfig config;

    private boolean done = false;

    public MigrationDemoSource(@Option("configuration") final SourceConfig config) {
        this.config = config;
    }

    @Producer
    public SourceConfig next() {
        if (done) {
            return null;
        }
        done = true;
        return config;
    }

}
