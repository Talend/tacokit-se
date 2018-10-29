package org.talend.components.processing.normalize;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonPointer;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.spi.JsonProvider;
import java.io.Serializable;
import java.util.function.Function;
import java.util.stream.Stream;

import static javax.json.stream.JsonCollectors.toJsonArray;
import static org.talend.sdk.component.api.component.Icon.IconType.NORMALIZE;

@Version
@Processor(name = "Normalize")
@Icon(NORMALIZE)
@Documentation("This component normalizes a column.")
public class Normalize implements Serializable {

    private final NormalizeConfiguration configuration;

    private final JsonBuilderFactory factory;

    private final JsonProvider provider;

    private transient String delimiter;

    private transient JsonPointer pointer;

    private transient Function<String, String> normalizers;

    public Normalize(@Option("configuration") final NormalizeConfiguration configuration, final JsonBuilderFactory factory,
            final JsonProvider provider) {
        this.configuration = configuration;
        this.factory = factory;
        this.provider = provider;
    }

    @ElementListener
    public void normalize(final JsonObject element, @Output final OutputEmitter<JsonObject> output) {
        ensureInit();

        final JsonValue jsonValue = pointer.getValue(element);
        if (JsonValue.NULL.equals(jsonValue)) {
            output.emit(element);
            return;
        }

        if (delimiter == null) {
            if (normalizers == null) {
                output.emit(element);
                return;
            }
            switch (jsonValue.getValueType()) {
            case ARRAY:
                jsonValue.asJsonArray().forEach(value -> output.emit(pointer.replace(element, doNormalize(value))));
                break;
            case STRING:
                output.emit(pointer.replace(element, doNormalize(jsonValue)));
                break;
            default:
                output.emit(element);
            }
            return;
        }

        doSplit(element, output, jsonValue);
    }

    private void doSplit(final JsonObject root, final OutputEmitter<JsonObject> output, final JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
        case ARRAY:
            jsonValue.asJsonArray().forEach(value -> doSplit(root, output, value));
            break;
        case STRING: {
            final String[] values = JsonString.class.cast(jsonValue).getString().split(delimiter);
            switch (values.length) {
            case 1:
                output.emit(pointer.replace(root, doNormalize(jsonValue)));
                return;
            default:
                Stream.of(values).forEach(value -> output.emit(pointer.replace(root, doNormalize(value))));
            }
            break;
        }
        default:
            output.emit(root);
        }
    }

    private void ensureInit() {
        if (pointer == null) {
            pointer = provider.createPointer(configuration.getColumnToNormalize());

            delimiter = NormalizeConfiguration.Delimiter.OTHER.equals(configuration.getFieldSeparator())
                    ? configuration.getOtherSeparator()
                    : configuration.getFieldSeparator().getDelimiter();

            normalizers = null;
            if (configuration.isDiscardTrailingEmptyStr()) {
                normalizers = src -> src.replaceAll("\\s+$", "");
            }
            if (configuration.isTrim()) {
                normalizers = normalizers == null ? String::trim : normalizers.andThen(String::trim);
            }
        }
    }

    private JsonValue doNormalize(final JsonValue jsonValue) {
        if (normalizers == null) {
            return jsonValue;
        }
        switch (jsonValue.getValueType()) {
        case ARRAY:
            return jsonValue.asJsonArray().stream().map(this::doNormalize).collect(toJsonArray());
        case STRING:
            final String string = JsonString.class.cast(jsonValue).getString();
            return doNormalize(string);
        default:
            return jsonValue;
        }
    }

    private JsonValue doNormalize(final String string) {
        return provider.createValue(normalizers.apply(string));
    }
}
