package org.talend.components.jdbc.derby;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.input.QueryInputTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Query Input - Derby")
@WithComponents(value = "org.talend.components.jdbc")
class DerbyQueryInputTest extends QueryInputTest implements DerbyTestInstance {

}
