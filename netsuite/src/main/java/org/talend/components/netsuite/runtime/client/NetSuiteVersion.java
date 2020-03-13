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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.talend.components.netsuite.datastore.NetSuiteDataStore.ApiVersion;
import org.talend.components.netsuite.runtime.NetSuiteErrorCode;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Hold NetSuite API version numbers.
 */
@Data
@ToString
@EqualsAndHashCode
public class NetSuiteVersion {

    private static final Pattern VERSION_PATTERN = Pattern.compile("((\\d+)\\.(\\d+))(\\.(\\d+))?");

    private static final Pattern ENDPOINT_URL_VERSION_PATTERN = Pattern.compile(".+\\/NetSuitePort_((\\d+)_(\\d+))(_(\\d+))?");

    /** First number of major version (year). */
    private int majorYear;

    /** Second number of major version (release). */
    private int majorRelease;

    /** Minor or patch version, can be <code>-1</code> if minor or patch version not specified. */
    private int minor;

    public NetSuiteVersion(int majorYear, int majorRelease) {
        this(majorYear, majorRelease, -1);
    }

    public NetSuiteVersion(int majorYear, int majorRelease, int minor) {
        this.majorYear = majorYear;
        this.majorRelease = majorRelease;
        this.minor = minor;
    }

    public NetSuiteVersion getMajor() {
        return new NetSuiteVersion(majorYear, majorRelease);
    }

    /**
     * Parse version.
     *
     * @param versionString version string
     * @return version object
     */
    public static NetSuiteVersion parseVersion(ApiVersion versionString) {
        Matcher matcher = VERSION_PATTERN.matcher(versionString.getVersion());
        if (matcher.matches()) {
            String sValue1 = matcher.group(2);
            String sValue2 = matcher.group(3);
            String sValue3 = matcher.group(5);
            try {
                int value1 = Integer.parseInt(sValue1);
                int value2 = Integer.parseInt(sValue2);
                int value3 = sValue3 != null ? Integer.parseInt(sValue3) : -1;
                return new NetSuiteVersion(value1, value2, value3);
            } catch (NumberFormatException e) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED), e.getMessage(), e);
            }
        }
        throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED), "Unsupported version");
    }

}
