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
package org.talend.components.extension.polling.mapperA;

import lombok.AllArgsConstructor;
import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.extension.polling.internal.PollingComponentExtensionTest;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;

import java.io.Serializable;

@Pollable(icon = "pollableSource", name = "MyPollable", resumeMethod = "resume")
@Emitter(family = "mytest", name = "BatchSource")
public class BatchSource implements Serializable {

    private final BatchConfig config;

    private int currentRow = 0;

    private int resumeInc = 0;

    public BatchSource(@Option("configuration") final BatchConfig config) {
        this.config = config;
    }

    public void resume(Object o) {
        resumeInc++;
    }

    @Producer
    public Data next() {
        // Only for test to wait enough for the polling
        try {
            Thread.sleep(PollingComponentExtensionTest.TEST_POLLING_DELAY + 50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (currentRow < PollingComponentExtensionTest.TEST_NB_EXPECTED_ROWS) {
            currentRow++;
            return new Data("Data/" + config.getParam1() + currentRow + "/" + resumeInc, config.getParam0() + currentRow);
        }

        return null;
    }

    @AllArgsConstructor
    @lombok.Data
    public static class Data {

        private final String valueStr;

        private final int valueInt;

    }

}