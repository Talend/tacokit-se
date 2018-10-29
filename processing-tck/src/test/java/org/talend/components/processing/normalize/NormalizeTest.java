package org.talend.components.processing.normalize;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.runtime.manager.chain.Job.components;

@WithComponents("org.talend.components.processing")
class NormalizeTest {

    @Injected
    private ComponentsHandler handler;

    @Service
    private JsonBuilderFactory factory;

    @Test
    void trim() {
        handler.setInputData(asList(
                factory.createObjectBuilder().add("nested", factory.createObjectBuilder().add("name", "The Test 1")).build(),
                factory.createObjectBuilder().add("nested", factory.createObjectBuilder().add("name", "  The Test 2  "))
                        .build()));
        components().component("input", "test://emitter")
                .component("normalize",
                        "Processing://Normalize?configuration.trim=true&configuration.columnToNormalize=/nested/name")
                .component("output", "test://collector").connections().from("input").to("normalize").from("normalize")
                .to("output").build().run();

        assertEquals(asList("The Test 1", "The Test 2"), handler.getCollectedData(JsonObject.class).stream()
                .map(obj -> obj.getJsonObject("nested").getString("name")).collect(toList()));
    }

    @Test
    void discardTrailingEmptyStr() {
        handler.setInputData(asList(
                factory.createObjectBuilder().add("nested", factory.createObjectBuilder().add("name", "  foo sp ")).build(),
                factory.createObjectBuilder().add("nested", factory.createObjectBuilder().add("name", "  The Test 2  "))
                        .build()));
        components().component("input", "test://emitter").component("normalize",
                "Processing://Normalize?configuration.discardTrailingEmptyStr=true&configuration.columnToNormalize=/nested/name")
                .component("output", "test://collector").connections().from("input").to("normalize").from("normalize")
                .to("output").build().run();

        assertEquals(asList("  foo sp", "  The Test 2"), handler.getCollectedData(JsonObject.class).stream()
                .map(obj -> obj.getJsonObject("nested").getString("name")).collect(toList()));
    }

    @Test
    void splitArray() {
        handler.setInputData(singletonList(factory.createObjectBuilder().add("nested",
                factory.createObjectBuilder().add("name", factory.createArrayBuilder().add("  first").add("second  ").build()))
                .build()));
        components().component("input", "test://emitter")
                .component("normalize",
                        "Processing://Normalize?configuration.trim=true&configuration.columnToNormalize=/nested/name")
                .component("output", "test://collector").connections().from("input").to("normalize").from("normalize")
                .to("output").build().run();

        assertEquals(asList("first", "second"), handler.getCollectedData(JsonObject.class).stream()
                .map(obj -> obj.getJsonObject("nested").getString("name")).collect(toList()));
    }

    @Test
    void splitString() {
        handler.setInputData(singletonList(factory.createObjectBuilder()
                .add("nested", factory.createObjectBuilder().add("name", "  first;second  ")).build()));
        components().component("input", "test://emitter")
                .component("normalize",
                        "Processing://Normalize?configuration.trim=true&configuration.columnToNormalize=/nested/name")
                .component("output", "test://collector").connections().from("input").to("normalize").from("normalize")
                .to("output").build().run();

        assertEquals(asList("first", "second"), handler.getCollectedData(JsonObject.class).stream()
                .map(obj -> obj.getJsonObject("nested").getString("name")).collect(toList()));
    }
}
