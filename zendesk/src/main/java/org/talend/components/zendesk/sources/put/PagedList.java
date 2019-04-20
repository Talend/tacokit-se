package org.talend.components.zendesk.sources.put;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PagedList<T> {

    private final int pageSize;

    private final Iterator<T> listIterator;

    public PagedList(List<T> list, int pageSize) {
        this.listIterator = list.iterator();
        this.pageSize = pageSize;
    }

    public List<T> getNextPage() {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < pageSize; i++) {
            if (listIterator.hasNext()) {
                result.add(listIterator.next());
            } else {
                break;
            }
        }
        return result.isEmpty() ? null : result;
    }

}
