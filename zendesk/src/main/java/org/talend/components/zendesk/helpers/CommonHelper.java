package org.talend.components.zendesk.helpers;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.sources.Reject;
import org.talend.components.zendesk.sources.get.InputIterator;
import org.talend.components.zendesk.sources.get.ZendeskGetConfiguration;
import org.talend.sdk.component.api.processor.OutputEmitter;

import javax.json.JsonObject;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class CommonHelper {

    public static void processException(Exception e, JsonObject record, OutputEmitter<Reject> reject) {
        log.warn(e.getMessage());
        reject.emit(new Reject(e.getMessage(), record));
    }

    public static long[] toPrimitives(Long... objects) {
        long[] primitives = new long[objects.length];
        for (int i = 0; i < objects.length; i++) {
            primitives[i] = objects[i];
        }
        return primitives;
    }

    public static InputIterator getInputIterator(ZendeskHttpClientService zendeskHttpClientService,
            ZendeskGetConfiguration configuration, Messages i18n) {
        InputIterator itemIterator;
        switch (configuration.getDataSet().getSelectionType()) {
        case REQUESTS:
            itemIterator = zendeskHttpClientService.getRequests(configuration.getDataSet().getDataStore());
            break;
        case TICKETS:
            itemIterator = zendeskHttpClientService.getTickets(configuration);
            break;
        default:
            throw new RuntimeException(i18n.UnknownTypeException());
        }
        return itemIterator;
    }

}
