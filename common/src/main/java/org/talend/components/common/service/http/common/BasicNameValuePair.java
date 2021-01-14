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
package org.talend.components.common.service.http.common;

import lombok.EqualsAndHashCode;

import java.io.Serializable;

@EqualsAndHashCode(of = { "name", "value" })
public class BasicNameValuePair implements Cloneable, Serializable {

    private static final long serialVersionUID = -6437800749411518984L;

    private final String name;

    private final String value;

    /**
     * Default Constructor taking a name and a value. The value may be null.
     *
     * @param name The name.
     * @param value The value.
     */
    public BasicNameValuePair(final String name, final String value) {
        super();
        if (name == null) {
            throw new IllegalArgumentException("Name may not be null");
        }
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }

    public String toString() {
        // don't call complex default formatting for a simple toString

        if (this.value == null) {
            return name;
        } else {
            int len = this.name.length() + 1 + this.value.length();
            CharArrayBuffer buffer = new CharArrayBuffer(len);
            buffer.append(this.name);
            buffer.append("=");
            buffer.append(this.value);
            return buffer.toString();
        }
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
