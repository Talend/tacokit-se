package org.talend.components.rest.service;

import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.Dataset;
import org.talend.components.rest.configuration.Datastore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.rest")
class ApiDescriptorServiceTest extends BaseTest {

    @Service
    private ApiDescriptorService apiDescriptorService;

    @Injected
    private BaseComponentsHandler handler;

    @Test
    void getBase() {
        // Inject needed services
        handler.injectServices(this);

        Datastore ds = new Datastore();
        ds.setUseDescriptor(true);
        ds.setDescriptorUrl(
                "https://raw.githubusercontent.com/OAI/OpenAPI-Specification/master/examples/v2.0/json/petstore.json");
        SuggestionValues suggestions = apiDescriptorService.getBase(ds);
    }

    @Test
    void autoload() {
        // Inject needed services
        handler.injectServices(this);

        Datastore dso = new Datastore();
        dso.setUseDescriptor(true);
        dso.setDescriptorUrl(
                "https://raw.githubusercontent.com/OAI/OpenAPI-Specification/master/examples/v2.0/json/petstore.json");
        dso.setBase("http://petstore.swagger.io/v1");

        Dataset dse = new Dataset();
        dse.setDatastore(dso);
        dse.setResource("/pets/{petId}");

        dse = apiDescriptorService.autoload(dse);
    }

}