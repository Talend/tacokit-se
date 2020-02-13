/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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

import java.nio.charset.Charset;

public class ByteArrayBuilder {

    private Charset charset;

    private StringBuilder builder;

    public ByteArrayBuilder(int size) {
        builder = new StringBuilder(size);
    }

    public void reset() {
        builder.delete(0, builder.length());
    }

    public ByteArrayBuilder append(String s) {
        this.builder.append(s);
        return this;
    }

    public ByteArrayBuilder append(char[] s) {
        this.builder.append(s);
        return this;
    }

    public void charset(Charset charset) {
        this.charset = charset;
    }

    public byte[] toByteArray() {
        return builder.toString().getBytes(this.charset);
    }

    public String toString() {
        return builder.toString();
    }

}
