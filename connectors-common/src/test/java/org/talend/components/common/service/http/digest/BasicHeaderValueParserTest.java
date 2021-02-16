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
package org.talend.components.common.service.http.digest;

import org.junit.jupiter.api.Test;
import org.talend.components.common.service.http.common.BasicHeader;
import org.talend.components.common.service.http.common.BasicHeaderValueParser;
import org.talend.components.common.service.http.common.BasicNameValuePair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BasicHeaderValueParserTest {

    @Test
    void parseWWWAuthenticateBasic() {
        Map<String, String> expected = new HashMap<>();
        expected.put("WWW-Authenticate-Type", "Basic");
        expected.put("realm", "My Realm");
        String challenge = "Basic realm=\"My Realm\"";
        parseWWWAuthenticate(expected, challenge);
    }

    @Test
    void parseWWWAuthenticateDigest() {
        Map<String, String> expected = new HashMap<>();
        expected.put("WWW-Authenticate-Type", "Digest");
        expected.put("realm", "me@mydomain.com");
        expected.put("nonce", "321205f334fa396833187ad546560cbe");
        expected.put("qop", "auth, auth-int");
        expected.put("opaque", "a7e6ee56656ddb8d654a7f9344c52111");
        expected.put("algorithm", "MD5");
        expected.put("stale", "FALSE");

        String challenge = "Digest realm=\"me@mydomain.com\", nonce=\"321205f334fa396833187ad546560cbe\", qop=\"auth, auth-int\", opaque=\"a7e6ee56656ddb8d654a7f9344c52111\", algorithm=MD5, stale=FALSE";
        parseWWWAuthenticate(expected, challenge);
    }

    @Test
    void parseWWWAuthenticateUnsupertedType() {
        Map<String, String> expected = new HashMap<>();
        expected.put("WWW-Authenticate-Type", "UnsuportedType");
        expected.put("realm", "me@mydomain.com");
        expected.put("nonce", "321205f334fa396833187ad546560cbe");
        expected.put("qop", "auth, auth-int");
        expected.put("opaque", "a7e6ee56656ddb8d654a7f9344c52111");
        expected.put("algorithm", "MD5");
        expected.put("stale", "FALSE");
        String challenge = "UnsuportedType realm=\"me@mydomain.com\", nonce=\"321205f334fa396833187ad546560cbe\", qop=\"auth, auth-int\", opaque=\"a7e6ee56656ddb8d654a7f9344c52111\", algorithm=MD5, stale=FALSE";
        parseWWWAuthenticate(expected, challenge);
    }

    @Test
    void parseWWWAuthenticateUnsupertedTypeWithSpaces() {
        Map<String, String> expected = new HashMap<>();
        expected.put("WWW-Authenticate-Type", "UnsuportedType");
        expected.put("realm", "me@mydomain.com");
        expected.put("nonce", "321205f334fa396833187ad546560cbe");
        expected.put("qop", "auth, auth-int");
        expected.put("opaque", "a7e6ee56656ddb8d654a7f9344c52111");
        expected.put("algorithm", "MD5");
        expected.put("stale", "FALSE");
        String challenge = "    UnsuportedType     realm=\"me@mydomain.com\"   ,    nonce=\"321205f334fa396833187ad546560cbe\",    qop=\"auth, auth-int\",    opaque=\"a7e6ee56656ddb8d654a7f9344c52111\",    algorithm=MD5   ,   stale=FALSE   ";
        parseWWWAuthenticate(expected, challenge);
    }

    @Test
    void parseWWWAuthenticateNoType() {
        Map<String, String> expected = new HashMap<>();
        expected.put("realm", "me@mydomain.com");
        expected.put("nonce", "321205f334fa396833187ad546560cbe");
        expected.put("qop", "auth, auth-int");
        expected.put("opaque", "a7e6ee56656ddb8d654a7f9344c52111");
        expected.put("algorithm", "MD5");
        expected.put("stale", "FALSE");
        String challenge = "realm=\"me@mydomain.com\", nonce=\"321205f334fa396833187ad546560cbe\", qop=\"auth, auth-int\", opaque=\"a7e6ee56656ddb8d654a7f9344c52111\", algorithm=MD5, stale=FALSE";
        parseWWWAuthenticate(expected, challenge);
    }

    @Test
    void parseOtherHeaderWithType() {
        // Type of header other than "WWW-Authenticate" must be ignored
        Map<String, String> expected = new HashMap<>();
        expected.put("Basic realm", "me@mydomain.com");
        expected.put("nonce", "321205f334fa396833187ad546560cbe");
        expected.put("qop", "auth, auth-int");
        expected.put("opaque", "a7e6ee56656ddb8d654a7f9344c52111");
        expected.put("algorithm", "MD5");
        expected.put("stale", "FALSE");
        String challenge = "Basic realm=\"me@mydomain.com\", nonce=\"321205f334fa396833187ad546560cbe\", qop=\"auth, auth-int\", opaque=\"a7e6ee56656ddb8d654a7f9344c52111\", algorithm=MD5, stale=FALSE";
        parseHeader("MyHeader", expected, challenge);
    }

    private void parseWWWAuthenticate(Map<String, String> expected, String challenge) {
        parseHeader("WWW-Authenticate", expected, challenge);
    }

    private void parseHeader(final String headerName, final Map<String, String> expected, final String challenge) {
        BasicHeader authChallenge = new BasicHeader(headerName, challenge);
        BasicNameValuePair[] nameValuePairs = BasicHeaderValueParser.parseParameters(authChallenge, new BasicHeaderValueParser());

        assertEquals(expected.size(), nameValuePairs.length);
        expected.entrySet().stream().forEach(k -> {
            assertTrue(existsOnce(nameValuePairs, k.getKey()));
            assertEquals(k.getValue(), getByName(nameValuePairs, k.getKey()).getValue());
        });
    }

    private boolean existsOnce(BasicNameValuePair[] pairs, String name) {
        List<BasicNameValuePair> list = Arrays.asList(pairs).stream().filter(p -> name.equals(p.getName()))
                .collect(Collectors.toList());
        return list.size() == 1;
    }

    private BasicNameValuePair getByName(BasicNameValuePair[] pairs, String name) {
        return Arrays.stream(pairs).filter(p -> name.equals(p.getName())).findFirst().orElse(new BasicNameValuePair("", ""));
    }

}