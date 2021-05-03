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
package org.talend.components.google.storage.service;

import java.io.Serializable;
import java.util.UUID;

/**
 * Generator name for blob.
 * To be sure several output connector running in parallel don't overwrite each others
 * on same blob, it generated before a unique name.
 */
public class BlobNameBuilder implements Serializable {

    private static final long serialVersionUID = -3957291260058538741L;

    private static final int UUID_STRING_SIZE = 36;

    public String generateName(final String originalName) {
        final int extensionStart = originalName.lastIndexOf('.');

        final String generatedName;
        if (extensionStart < 0) {
            generatedName = originalName + '_' + UUID.randomUUID();
        } else {
            generatedName = originalName.substring(0, extensionStart) + '_' + UUID.randomUUID()
                    + originalName.substring(extensionStart);
        }
        return generatedName;
    }

    public boolean isGenerated(final String begin, final String name) {

        final int extensionPos = this.lastOrLength(begin, '.');

        final String start = begin.substring(0, extensionPos);
        final String extension = begin.substring(extensionPos);

        return name.startsWith(start + '_') //
                && name.length() == begin.length() + 1 + UUID_STRING_SIZE //
                && this.isUUID(name.substring(start.length() + 1, name.length() - extension.length())); //
    }

    public String revert(final String generatedName) {
        if (generatedName == null) {
            return null;
        }
        final int extensionPos = this.lastOrLength(generatedName, '.');

        int separatorPos = generatedName.lastIndexOf('_', extensionPos);
        if (separatorPos < 0) {
            return generatedName;
        }
        final String uuidCandidate = generatedName.substring(separatorPos + 1, extensionPos);
        if (!this.isUUID(uuidCandidate)) {
            return generatedName;
        }

        final String revertedName = generatedName.substring(0, separatorPos) + generatedName.substring(extensionPos);

        return revertedName;
    }

    private int lastOrLength(String name, char element) {
        final int pos = name.lastIndexOf(element);
        return pos < 0 ? name.length() : pos;
    }

    private boolean isUUID(final String uuidString) {
        try {
            UUID.fromString(uuidString);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }
}
