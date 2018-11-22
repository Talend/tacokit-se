package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.InsertTests;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output insert  - MySQL")
@WithComponents(value = "org.talend.components.jdbc")
class MySQLInsertTest extends InsertTests implements MySQLTestInstance {

}
