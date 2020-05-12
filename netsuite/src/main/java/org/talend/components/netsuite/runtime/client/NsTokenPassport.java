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
package org.talend.components.netsuite.runtime.client;

import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.time.Instant;
import java.util.Base64;

@Data
public class NsTokenPassport {

    private static final int LENGTH_OF_ALPHA_NUMERIC = 40;

    protected String account;

    protected String consumerKey;

    protected String token;

    protected String nonce;

    protected long timestamp;

    protected NsTokenPassportSignature signature;

    private String secret;

    public NsTokenPassport(String account, String consumerKey, String consumerSecret, String token, String tokenSecret) {
        this.account = account;
        this.consumerKey = consumerKey;
        this.token = token;
        this.secret = String.join("&", consumerSecret, tokenSecret);
        this.signature = new NsTokenPassportSignature();
        signature.setAlgorithm(NsTokenPassportSignature.Algorithm.Hmac_SHA256);
    }

    public String refresh() {
        try {
            this.nonce = generateNonce();
            this.timestamp = Instant.now().getEpochSecond();
            String baseString = String.join("&", account, consumerKey, token, nonce, String.valueOf(timestamp));
            return computeShaHash(baseString, secret, signature.getAlgorithm().getAlgorithmString());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private String generateNonce() {
        return RandomStringUtils.randomAlphanumeric(LENGTH_OF_ALPHA_NUMERIC);
    }

    private String computeShaHash(String baseString, String key, String algorithm) throws Exception {
        byte[] bytes = key.getBytes();
        SecretKeySpec mySigningKey = new SecretKeySpec(bytes, algorithm);
        Mac messageAuthenticationCode = Mac.getInstance(algorithm);
        messageAuthenticationCode.init(mySigningKey);
        byte[] hash = messageAuthenticationCode.doFinal(baseString.getBytes());
        String result = Base64.getEncoder().encodeToString(hash);
        return result;
    }
}
