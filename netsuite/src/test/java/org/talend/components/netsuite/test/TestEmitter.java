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
package org.talend.components.netsuite.test;

import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

@Version
@Icon(Icon.IconType.SAMPLE)
@Emitter(name = NetSuiteBaseTest.TEST_EMITTER, family = NetSuiteBaseTest.TEST_FAMILY_NAME)
public class TestEmitter implements Serializable {

    private static Queue<Record> data = new LinkedList<>();

    public static void addRecord(Record record) {
        data.add(record);
    }

    @Producer
    public Record next() {
        return data.poll();
    }

    public static void reset() {
        data = new LinkedList<>();
    }

}
