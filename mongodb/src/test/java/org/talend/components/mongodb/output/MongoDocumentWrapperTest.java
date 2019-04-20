/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

package org.talend.components.mongodb.output;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoDocumentWrapperTest {

    @Test
    public void testPutValues() {
        MongoDocumentWrapper wrapper = new MongoDocumentWrapper();

        Document doc = Document.parse("{\"a\":1, \"b\":{\"c\":\"ValueC\", \"d\":{\"e\":\"ValueE\"}}}");

        wrapper.put(null, "a", 1);
        wrapper.put(new OutputMapping("c", "b", false), "c", "ValueC");
        wrapper.put(new OutputMapping("e", "b.d", false), "e", "ValueE");

        assertEquals(doc, wrapper.getObject(), "MongoDocumentWrapper returns wrong document");
    }

    @Test
    public void testPutKeyValues() {
        MongoDocumentWrapper wrapper = new MongoDocumentWrapper();

        Document doc = Document.parse("{\"a\":1, \"b.c\":\"ValueC\", \"b.d.e\":\"ValueE\"}");

        wrapper.putkeyNode(null, "a", 1);
        wrapper.putkeyNode(new OutputMapping("c", "b", false), "c", "ValueC");
        wrapper.putkeyNode(new OutputMapping("e", "b.d", false), "e", "ValueE");

        assertEquals(doc, wrapper.getObject(), "MongoDocumentWrapper returns wrong key values document");
    }

}
