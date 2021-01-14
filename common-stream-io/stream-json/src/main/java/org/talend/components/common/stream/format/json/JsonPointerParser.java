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
package org.talend.components.common.stream.format.json;

import java.util.Collections;
import java.util.Iterator;

import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

/**
 * Parse a json object with json pointer syntax.
 * This class allow to parse a json source progressivly
 * (low memory consuming / start quicly).
 */
public class JsonPointerParser {

    /** root of json pointer */
    private final JsonGetter rootGetter;

    private JsonPointerParser(JsonGetter rootGetter) {
        this.rootGetter = rootGetter;
    }

    /**
     * Build an instance of parser with json pointer string.
     * string respect RFC-6901 : https://tools.ietf.org/html/rfc6901
     * 
     * @param jsonPointerSpec : string of form "/field1/field2" (see RFC-6901)
     * @return parser of json object.
     */
    public static JsonPointerParser of(String jsonPointerSpec) {
        JsonGetter rootGetter;
        if (jsonPointerSpec != null && jsonPointerSpec.length() > 1) {
            final String[] elements = jsonPointerSpec.split("/");
            JsonGetter current = null;
            for (int index = elements.length - 1; index > 0; index--) {
                current = new JsonGetter(elements[index], current);
            }
            rootGetter = current;
        } else {
            rootGetter = new JsonGetter(null, null);
        }
        return new JsonPointerParser(rootGetter);
    }

    /**
     * Search iterator on json.
     * 
     * @param parser : json parser.
     * @return iterator on json value.
     */
    public Iterator<JsonValue> values(JsonParser parser) {

        final Iterator<JsonValue> valuesIterator;
        if (rootGetter.get(parser) && parser.hasNext()) {
            Event evt = parser.next();
            if (evt == Event.START_ARRAY) {
                valuesIterator = new JsonIterator(parser);
            } else {
                final JsonValue lonelyObject = parser.getValue();
                valuesIterator = Collections.singletonList(lonelyObject).iterator();
            }
        } else {
            valuesIterator = Collections.emptyIterator();
        }

        return valuesIterator;
    }

    /**
     * Iterator on Json Value.
     * (iterating on all object of array if jsonpointer point an array, single object otherwire).
     */
    static class JsonIterator implements Iterator<JsonValue> {

        /** current json value */
        private JsonValue current;

        /** if array, is end reached */
        private boolean endArrayReached = false;

        /** point to the current json value */
        private final JsonParser parser;

        public JsonIterator(JsonParser parser) {
            this.parser = parser;
            this.current = this.findNext(parser);
        }

        @Override
        public boolean hasNext() {
            return this.current != null;
        }

        @Override
        public JsonValue next() {
            final JsonValue result = this.current;
            this.current = this.findNext(parser);
            return result;
        }

        /**
         * Search next value.
         * 
         * @param parser : json parser.
         * @return value if exist, null otherwise.
         */
        private JsonValue findNext(JsonParser parser) {
            if (this.endArrayReached || !parser.hasNext()) {
                this.parser.close();
                return null;
            }
            Event evt = parser.next();
            if (evt == Event.END_ARRAY) {
                this.endArrayReached = true;
                this.parser.close();
                return null;
            }
            return parser.getValue();
        }
    }

    /**
     * Class that translate JsonPointer.
     */
    static class JsonGetter {

        /** current element name of json-pointer */
        private final String elementName;

        /** next element of json pointer (if exist) */
        private final JsonGetter next;

        public JsonGetter(String elementName, JsonGetter next) {
            this.elementName = elementName;
            this.next = next;
        }

        /**
         * Recursivly find keys.
         * 
         * @param parser : parser for json object (will point on good place if finded).
         * @return true if finded.
         */
        public boolean get(JsonParser parser) {
            boolean keyFinded = this.findKey(parser);
            if (keyFinded) {
                if (this.next == null) {
                    return true;
                } else {
                    return this.next.findKey(parser);
                }
            }
            return false;
        }

        /**
         * Find key name in json parser that correspond to current element name.
         * 
         * @param parser : parser for json object (will point on good place if finded).
         * @return true if finded.
         */
        private boolean findKey(JsonParser parser) {
            if (this.elementName == null || "".equals(this.elementName)) {
                return true;
            }
            boolean finded = false;
            Event event = null;

            while (!finded && parser.hasNext()) {
                event = parser.next();
                if (event == Event.KEY_NAME) {
                    String key = parser.getString();
                    finded = this.elementName.equals(key);
                }
            }
            return finded;
        }
    }

}
