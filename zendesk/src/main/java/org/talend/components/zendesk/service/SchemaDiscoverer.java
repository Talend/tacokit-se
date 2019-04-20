package org.talend.components.zendesk.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.helpers.CommonHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.sources.get.InputIterator;
import org.talend.components.zendesk.sources.get.ZendeskGetConfiguration;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class SchemaDiscoverer implements Serializable {

    @Service
    private Messages i18n;

    @Service
    private ZendeskHttpClientService httpClientService;

    public List<String> getColumns(ZendeskGetConfiguration configuration) {
        List<String> result = new ArrayList<>();
        try {
            InputIterator itemIterator = CommonHelper.getInputIterator(httpClientService, configuration, i18n);

            if (itemIterator.hasNext()) {
                JsonValue val = itemIterator.next();
                val.asJsonObject().forEach((columnName, value) -> result.add(columnName));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }
}