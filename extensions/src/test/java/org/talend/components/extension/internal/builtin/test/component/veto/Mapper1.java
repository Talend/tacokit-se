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
package org.talend.components.extension.internal.builtin.test.component.veto;

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.record.Record;

@PartitionMapper(name = "Mapper1")
public class Mapper1 implements Serializable {

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<Mapper1> split() {
        return singletonList(this);
    }

    @Emitter
    public Input1 createWorker() {
        return new Input1();
    }

    public static class Input1 {

        @Producer
        public Record next() {
            return null;
        }
    }
}
