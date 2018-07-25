package org.talend.components.jms.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.junit5.WithComponents;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@WithComponents("org.talend.components.jms") // component package
class ActionServiceTest {

    @Service
    private ActionService actionService;

    @Test
    @DisplayName("DynamicValue - Load Providers")
    void loadSupportedDataBaseTypes() {
        final Values values = actionService.loadSupportedJMSProviders();
        assertNotNull(values);
        assertEquals(1, values.getItems().size());
        assertEquals(asList("ACTIVEMQ"), values.getItems().stream().map(Values.Item::getId).collect(toList()));
    }

}
