// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.output;

import javax.json.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.MarketoBaseTest;

public class MarketoProcessorBaseTest extends MarketoBaseTest {

    protected static final String FAIL_REJECT = "Should not have a reject";

    protected static final String FAIL_MAIN = "Should not have a main";

    MarketoProcessor processor;

    JsonObject data, dataNotExist;

    protected transient static final Logger LOG = LoggerFactory.getLogger(MarketoProcessorBaseTest.class);

}
