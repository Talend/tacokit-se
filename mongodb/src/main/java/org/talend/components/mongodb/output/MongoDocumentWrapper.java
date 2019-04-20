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

public class MongoDocumentWrapper {

    private Document object;

    public MongoDocumentWrapper() {
        this.object = new Document();
    }

    // Put value to embedded document
    // If have no embedded document, put the value to root document
    public void put(OutputMapping mapping, String curentName, Object value) {
        String parentNode = mapping == null ? null : mapping.getParentNodePath();
        boolean removeNullField = mapping == null ? false : mapping.isRemoveNullField();
        if (removeNullField && value == null) {
            return;
        }
        if (parentNode == null || "".equals(parentNode)) {
            object.put(curentName, value);
        } else {
            String[] objNames = parentNode.split("\\.");
            Document lastNode = getParentNode(parentNode, objNames.length - 1);
            lastNode.put(curentName, value);
            Document parenttNode = null;
            for (int i = objNames.length - 1; i >= 0; i--) {
                parenttNode = getParentNode(parentNode, i - 1);
                parenttNode.put(objNames[i], lastNode);
                lastNode = clone(parenttNode);
            }
            object = lastNode;
        }
    }

    private Document clone(Document source) {
        Document to = new Document();
        for (java.util.Map.Entry<String, Object> cur : source.entrySet()) {
            to.append(cur.getKey(), cur.getValue());
        }
        return to;
    }

    // Get node(embedded document) by path configuration
    public Document getParentNode(String parentNode, int index) {
        Document document = object;
        if (parentNode == null || "".equals(parentNode)) {
            return object;
        } else {
            String[] objNames = parentNode.split("\\.");
            for (int i = 0; i <= index; i++) {
                document = (Document) document.get(objNames[i]);
                if (document == null) {
                    document = new Document();
                    return document;
                }
                if (i == index) {
                    break;
                }
            }
            return document;
        }
    }

    public void putkeyNode(OutputMapping mapping, String currentName, Object value) {
        String parentNode = mapping == null ? null : mapping.getParentNodePath();
        if (mapping == null || parentNode == null || "".equals(parentNode) || ".".equals(parentNode)) {
            put(mapping, currentName, value);
        } else {
            put(new OutputMapping(currentName, "", mapping.isRemoveNullField()), parentNode + "." + currentName, value);
        }
    }

    public Document getObject() {
        return this.object;
    }

}
