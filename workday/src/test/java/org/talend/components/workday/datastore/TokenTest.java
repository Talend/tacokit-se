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
package org.talend.components.workday.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

class TokenTest {

    @Test
    void isTooOld() {
        Token token = new Token("accesToken", "bearer", Instant.now());
        Assertions.assertTrue(token.isTooOld());

        token = new Token("accesToken", "bearer", Instant.now().plus(20, ChronoUnit.SECONDS));
        Assertions.assertTrue(token.isTooOld());

        Token token1 = new Token("accesToken", "bearer", Instant.now().minus(120, ChronoUnit.MINUTES));
        Assertions.assertTrue(token1.isTooOld());

        Token token2 = new Token("accesToken", "bearer", Instant.now().plus(120, ChronoUnit.MINUTES));
        Assertions.assertFalse(token2.isTooOld());
    }
}