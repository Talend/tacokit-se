package org.talend.components.zendesk.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import java.io.IOException;
import java.io.StringReader;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class JsonHelper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(StdDateFormat.getDateTimeInstance());
    }

    public static <T> T toInstance(JsonObject record, final Class<T> clazz) {
        try {
            return objectMapper.readerFor(clazz).readValue(record.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonObject toJsonObject(Object obj, JsonReaderFactory jsonReaderFactory) {
        try {
            String jsonStr = objectMapper.writeValueAsString(obj);
            JsonObject jsonObject = jsonReaderFactory.createReader(new StringReader(jsonStr)).readObject();
            return jsonObject;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
