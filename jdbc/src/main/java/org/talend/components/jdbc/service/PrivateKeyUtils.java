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
package org.talend.components.jdbc.service;

import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;

public final class PrivateKeyUtils {

    static {
        // Define Bouncy Castle Provider for Snowflake Key Pair authentication
        Security.addProvider(new BouncyCastleProvider());
    }

    private PrivateKeyUtils() {
    }

    public static PrivateKey getPrivateKey(String privateKey, String privateKeyPassword, final I18nMessage i18nMessage) {
        try {
            return privateKey.contains("ENCRYPTED") ? getFromEncrypted(privateKey, privateKeyPassword)
                    : getFromRegular(privateKey);
        } catch (PKCSException pkcse) {
            throw new IllegalArgumentException(i18nMessage.errorPrivateKeyPasswordIncorrect(), pkcse);
        } catch (InvalidKeySpecException | IOException | OperatorCreationException | NoSuchAlgorithmException
                | DecoderException e) {
            throw new IllegalArgumentException(i18nMessage.errorPrivateKeyIncorrect(), e);
        }
    }

    private static PrivateKey getFromEncrypted(String privateKey, String privateKeyPassword)
            throws IOException, OperatorCreationException, PKCSException {
        PKCS8EncryptedPrivateKeyInfo pkcs8EncryptedPrivateKeyInfo = new PKCS8EncryptedPrivateKeyInfo(
                decodeString(replaceGeneratedExtraString(privateKey, true)));
        InputDecryptorProvider inputDecryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder().setProvider("BC")
                .build(ofNullable(privateKeyPassword).map(String::toCharArray).orElse(new char[0]));
        PrivateKeyInfo privateKeyInfo = pkcs8EncryptedPrivateKeyInfo.decryptPrivateKeyInfo(inputDecryptorProvider);
        return new JcaPEMKeyConverter().setProvider("BC").getPrivateKey(privateKeyInfo);
    }

    private static PrivateKey getFromRegular(String privateKey) throws InvalidKeySpecException, NoSuchAlgorithmException {
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodeString(replaceGeneratedExtraString(privateKey, false)));
        return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
    }

    private static byte[] decodeString(String privateKeyContent) {
        return Base64.decode(privateKeyContent);
    }

    private static String replaceGeneratedExtraString(String privateKey, boolean isEncrypted) {
        return isEncrypted
                ? privateKey.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "").replace("-----END ENCRYPTED PRIVATE KEY-----",
                        "")
                : privateKey.replace("-----BEGIN RSA PRIVATE KEY-----", "").replace("-----END RSA PRIVATE KEY-----", "")
                        .replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "");
    }
}
