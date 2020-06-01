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
package org.talend.components.ftp.service.ftpclient;

import lombok.Data;

import java.io.Serializable;
import java.util.Comparator;

@Data
public class GenericFTPFile implements Serializable {

    /**
     * Comparator to sort files based on size. Bigger is first
     */
    public static class GenericFTPFileSizeComparator implements Comparator<GenericFTPFile> {

        @Override
        public int compare(GenericFTPFile f1, GenericFTPFile f2) {
            if (f1 == f2) {
                return 0;
            }
            if (f1 == null) {
                return -1;
            }
            if (f2 == null) {
                return 1;
            }

            return Long.compare(f2.getSize(), f1.getSize());
        }
    }

    private String name;

    private long size;

    private boolean directory;
}
