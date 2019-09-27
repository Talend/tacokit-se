package org.talend.components.workday.service;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.bind.JsonbBuilder;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class WorkdayReaderTest {

    @Test
    void search() {
        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(),
                new HashMap<Class<?>, Object>() );

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, "https://auth.api.workday.com");

        WorkdayDataStore wds = ConfigHelper.buildDataStore();
        Token tk = provider.getAccessToken(wds);

        WorkdayReader reader = factory.create(WorkdayReader.class, "https://api.workday.com");
        String header = tk.getAuthorizationHeaderValue();
        Response<JsonObject> rs = reader.search(header, "common/v1/workers", 0, 50);

        Assertions.assertNotNull(rs, "reponse null");
        Assertions.assertEquals(2, rs.status() / 100, () -> "status = " + rs.status());
        JsonObject objet = rs.body();
        Assertions.assertNotNull(objet, "contenu de reponse null");
        int tot = objet.getInt("total");
        JsonArray array = objet.getJsonArray("data");
        int nbe = array.size();
        Assertions.assertTrue( nbe <= 50, () -> "nbe = " + nbe + " > 50");
    }
}