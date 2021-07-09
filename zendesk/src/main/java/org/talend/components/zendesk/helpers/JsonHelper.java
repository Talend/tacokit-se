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

import java.io.IOException;
import java.io.StringReader;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;

import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class JsonHelper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(StdDateFormat.getDateTimeInstance());
    }

    public static <T> T toInstance(Record record, final Class<T> clazz) {
        try {
            return objectMapper.readerFor(clazz).readValue(getJsonString(record));
        } catch (IOException e) {
            throw new ComponentException(e);
        }
    }

    public static String getJsonString(Record record) {
        String delegate = record.toString();
        log.debug("delegate: " + delegate);
        if (delegate.startsWith("AvroRecord")) {
            // To avoid import dependence of AvroRecord
            return delegate.substring(20, delegate.length() - 1);
        }
        return delegate;
    }

    public static JsonObject toJsonObject(Object obj, JsonReaderFactory jsonReaderFactory) {
        try {
            String jsonStr = objectMapper.writeValueAsString(obj);
            JsonObject jsonObject = jsonReaderFactory.createReader(new StringReader(jsonStr)).readObject();
            return jsonObject;
        } catch (JsonProcessingException e) {
            throw new ComponentException(e);
        }
    }

}
