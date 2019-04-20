package org.talend.components.zendesk;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.common.SelectionType;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.components.zendesk.common.ZendeskDataStore;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class ZendeskTestHelper {

    public static ZendeskDataSet getTicketDataSet(ZendeskDataStore dataStore) {
        ZendeskDataSet dataSet = new ZendeskDataSet();
        dataSet.setDataStore(dataStore);
        dataSet.setSelectionType(SelectionType.TICKETS);
        return dataSet;
    }
}
