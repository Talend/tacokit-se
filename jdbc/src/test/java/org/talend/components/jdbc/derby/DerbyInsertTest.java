package org.talend.components.jdbc.derby;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.InsertTests;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output insert  - Derby")
@WithComponents(value = "org.talend.components.jdbc")
class DerbyInsertTest extends InsertTests implements DerbyTestInstance {

}
