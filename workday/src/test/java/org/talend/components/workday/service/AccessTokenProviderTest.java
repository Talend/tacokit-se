package org.talend.components.workday.service;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

import javax.json.bind.JsonbBuilder;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class AccessTokenProviderTest {

    @Test
    void getAccessToken() {
        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(),
                new HashMap<Class<?>, Object>() );

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, "https://auth.api.workday.com");
        WorkdayDataStore wds = ConfigHelper.buildDataStore();

        Token tk = provider.getAccessToken(wds);
        Assertions.assertNotNull(tk);
        Assertions.assertEquals("Bearer", tk.getTokenType());
    }

    @Test
    void getAccessTokenError() {
        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(),
                new HashMap<Class<?>, Object>() );

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, "https://auth.api.workday.com");

        WorkdayDataStore wds = ConfigHelper.buildDataStore();
        wds.setClientSecret("fautSecret");

        Assertions.assertThrows(WorkdayException.class, () -> provider.getAccessToken(wds));
    }
}