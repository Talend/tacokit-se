package org.talend.components.mongodb.output;

import com.mongodb.Mongo;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;

@Processor(name = "output")
@Version(1)
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "MongoDBOutput")
public class MongoDBOutputProcessor {

    private final MongoDBOutputConfig config;

    private final I18nMessage i18n;

    public MongoDBOutputProcessor(@Option("configuration") final MongoDBOutputConfig config, final I18nMessage i18nMessage) {
        this.config = config;
        this.i18n = i18nMessage;
    }

    @ElementListener
//    public void elementListener(@Input final Record record) {
//        records.add(record);
//    }

    @PostConstruct
    public void init() {

    }

    @BeforeGroup
    public void beforeGroup() {
    }

    @AfterGroup
    public void afterGroup() {

    }

    @PreDestroy
    public void preDestroy() {

    }

}
