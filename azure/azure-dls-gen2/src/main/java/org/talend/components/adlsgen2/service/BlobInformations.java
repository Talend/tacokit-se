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
package org.talend.components.adlsgen2.service;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class BlobInformations {

    public String etag;

    public String group;

    public String lastModified;

    public String name;

    public String owner;

    public String permissions;

    private boolean exists = Boolean.FALSE;

    private String blobPath;

    private String fileName;

    private String directory;

    private Integer contentLength = 0;
}
