/**
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.components.mail.mapper;

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import javax.mail.Flags;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MailEmitterFlagConfigurationTest {

    @ParameterizedTest
    @CsvSource({ "SEEN,-,SEEN|", "FLAGGED,-,FLAGGED|", "ANSWERED,-,ANSWERED|", "CUSTOM,X-flag,|X-flag", })
    void build(final MailEmitter.MailFlag mailFlag, final String custom, final String expected) {
        final MailEmitter.FlagConfiguration configuration = new MailEmitter.FlagConfiguration();
        configuration.setFlag(mailFlag);
        configuration.setCustomFlag(custom);

        final Flags flags = configuration.build();
        assertEquals(expected, Stream.of(flags.getSystemFlags()).map(f -> {
            if (f == Flags.Flag.ANSWERED) {
                return "ANSWERED";
            }
            if (f == Flags.Flag.FLAGGED) {
                return "FLAGGED";
            }
            if (f == Flags.Flag.SEEN) {
                return "SEEN";
            }
            throw new IllegalStateException(f.toString());
        }).collect(joining(",")) + "|" + String.join(",", flags.getUserFlags()));
    }
}
