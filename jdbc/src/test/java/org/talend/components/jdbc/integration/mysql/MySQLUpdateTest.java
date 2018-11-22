package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.UpdateTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output update  - MySQL")
@WithComponents(value = "org.talend.components.jdbc")
class MySQLUpdateTest extends UpdateTest implements MySQLTestInstance {

}
