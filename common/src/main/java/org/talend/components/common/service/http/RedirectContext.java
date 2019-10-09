/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.common.service.http;

import lombok.Data;
import org.talend.sdk.component.api.service.http.Response;

import java.util.ArrayList;
import java.util.List;

@Data
public class RedirectContext {

    private final String base;

    private final Response response;

    private final Integer maxRedirect;

    private final Integer nbRedirect;

    private Integer nextNbRedirect = 0;

    private String nextUrl = null;

    private List<RedirectContext> history = new ArrayList<>();

    public RedirectContext(final Response response, final RedirectContext previous){
        this.response = response;
        this.base = previous.getBase();
        this.nbRedirect = previous.getNextNbRedirect();
        this.maxRedirect = previous.getMaxRedirect();
        this.history.addAll(previous.getHistory());
        this.history.add(0, this);
    }

    public RedirectContext(final String base, final Integer maxRedirect){
        this.response = null;
        this.maxRedirect = maxRedirect;
        this.nbRedirect = 0;
        this.base = base;
    }

    public boolean isRedirect() {
        return nextUrl != null;
    }

    public void setNewUrlAndIncreaseNbRedirection(String url){
        this.nextNbRedirect = this.nbRedirect + 1;
        this.nextUrl = url;
    }

}
