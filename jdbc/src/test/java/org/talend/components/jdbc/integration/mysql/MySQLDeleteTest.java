package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.derby.DerbyTestInstance;
import org.talend.components.jdbc.output.DeleteTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output delete  - MySQL")
@WithComponents(value = "org.talend.components.jdbc")
class MySQLDeleteTest extends DeleteTest implements DerbyTestInstance {

}
