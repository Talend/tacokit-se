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
package org.talend.components.marketo.dataset;

import static org.talend.components.marketo.service.UIActionService.ACTIVITIES_LIST;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ActivityType implements Serializable, CharSequence {

    @Option
    @Suggestable(value = ACTIVITIES_LIST, parameters = { "../../dataStore" })
    @Documentation("Activity")
    private String activity;

    @Override
    public int length() {
        return activity.length();
    }

    @Override
    public char charAt(int index) {
        return activity.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return subSequence(start, end);
    }
}
