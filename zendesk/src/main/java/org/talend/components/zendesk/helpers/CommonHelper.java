/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.zendesk.helpers;

import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.source.InputIterator;
import org.talend.components.zendesk.source.ZendeskInputMapperConfiguration;
import org.talend.sdk.component.api.exception.ComponentException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class CommonHelper {

    public static long[] toPrimitives(Long... objects) {
        long[] primitives = new long[objects.length];
        for (int i = 0; i < objects.length; i++) {
            primitives[i] = objects[i];
        }
        return primitives;
    }

    public static InputIterator getInputIterator(ZendeskHttpClientService zendeskHttpClientService,
            ZendeskInputMapperConfiguration configuration, Messages i18n) {
        InputIterator itemIterator;
        switch (configuration.getDataset().getSelectionType()) {
        case REQUESTS:
            itemIterator = zendeskHttpClientService.getRequests(configuration.getDataset().getDataStore());
            break;
        case TICKETS:
            itemIterator = zendeskHttpClientService.getTickets(configuration);
            break;
        default:
            throw new ComponentException(i18n.UnknownTypeException());
        }
        return itemIterator;
    }

}
