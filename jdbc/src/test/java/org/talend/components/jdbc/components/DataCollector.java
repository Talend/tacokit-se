/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.components;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

@Version
@Icon(Icon.IconType.SAMPLE)
@Processor(name = "DataCollector", family = "jdbcTest")
public class DataCollector implements Serializable {

    private static Queue<Record> data = new ConcurrentLinkedQueue<>();

    public DataCollector() {

    }

    @PostConstruct
    public void init() {

    }

    @ElementListener
    public void onElement(@Input final Record record) {
        data.add(record);
    }

    public static Queue<Record> getData() {
        return data;
    }

    public static void reset() {
        data = new ConcurrentLinkedQueue<>();
    }
}
