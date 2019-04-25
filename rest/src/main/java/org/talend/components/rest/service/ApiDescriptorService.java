// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.rest.service;

import com.restlet.definitions.rwadef40.Definition;
import com.restlet.definitions.translator.Translator;
import com.restlet.definitions.translator.destination.rwadef.Rwadef40Format;
import com.restlet.definitions.translator.sources.openapi30.OpenApi30Source;
import com.restlet.definitions.utils.utilitary.MediaType;
import com.restlet.definitions.utils.validation.TranslationResult;
import org.talend.components.rest.configuration.Dataset;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.Param;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.update.Update;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ApiDescriptorService {

    private Definition getRwadef(String descriptorUrl) {
        Definition descriptionRwadef = null;
        try (InputStream descriptor = new java.net.URL(descriptorUrl).openStream()) {
            /*
             * TranslationResult<Definition> translator = new Translator<Definition>()
             * .from(Swagger20Source.fromStream(descriptor, MediaType.JSON)).to(Rwadef40Format.class).translate();
             */
            TranslationResult<Definition> translator = new Translator<Definition>()
                    .from(OpenApi30Source.fromStream(descriptor, MediaType.JSON)).to(Rwadef40Format.class).translate();

            descriptionRwadef = translator.getResult();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return descriptionRwadef;
    }

    @Suggestions("getBase")
    public SuggestionValues getBase(@Option final Datastore datastore) {
        final boolean useDescriptor = datastore.isUseDescriptor();
        final String descriptorUrl = datastore.getDescriptorUrl();

        final SuggestionValues values = new SuggestionValues();

        if (!useDescriptor) {
            values.setItems(Collections.emptyList());
            return values;
        }

        Definition descriptionRwadef = this.getRwadef(descriptorUrl);
        if (descriptionRwadef == null) {
            throw new IllegalArgumentException("Descriptor url can't be loaded : " + descriptorUrl);
        }

        List<SuggestionValues.Item> items = new ArrayList<>();
        descriptionRwadef.contract.baseUrls.forEach((k, v) -> items.add(new SuggestionValues.Item(v.url, v.url)));

        values.setItems(items);

        return values;
    }

    @Suggestions("getPaths")
    public SuggestionValues getPaths(@Option final Datastore datastore) {

        final boolean useDescriptor = datastore.isUseDescriptor();
        final String descriptorUrl = datastore.getDescriptorUrl();

        final SuggestionValues values = new SuggestionValues();

        if (!useDescriptor) {
            values.setItems(Collections.emptyList());
            return values;
        }

        Definition descriptionRwadef = this.getRwadef(descriptorUrl);
        if (descriptionRwadef == null) {
            throw new IllegalArgumentException("Descriptor url can't be loaded : " + descriptorUrl);
        }

        List<SuggestionValues.Item> items = new ArrayList<>();
        descriptionRwadef.contract.resources.forEach((k, v) -> items.add(new SuggestionValues.Item(v.path, v.path)));
        values.setItems(items);

        return values;

    }

    @Update("autoload")
    public Dataset autoload(@Option Dataset dataset) {
        if (!dataset.getDatastore().isUseDescriptor() || dataset.getDatastore().getDescriptorUrl().trim().isEmpty()) {
            return dataset;
        }

        String descriptorUrl = dataset.getDatastore().getDescriptorUrl();
        Definition descriptionRwadef = this.getRwadef(descriptorUrl);

        String base = dataset.getDatastore().getBase();
        String resource = dataset.getResource();

        List<Param> neededPathParams = new ArrayList<>();

        descriptionRwadef.contract.resources.values().stream().filter(r -> resource.equals(r.path))
                .forEach(r -> r.pathVariables.values().stream().forEach(v -> neededPathParams.add(new Param(v.getName(), ""))));

        // keep existing configuration
        List<Param> existing = dataset.getPathParams();
        List<String> existingKeys = existing.stream().map(Param::getKey).collect(Collectors.toList());
        neededPathParams.stream().forEach(p -> {
            if (!existingKeys.contains(p.getKey())) {
                existing.add(p);
            }
        });

        dataset.setPathParams(existing);
        dataset.setHasPathParams(existing.size() > 0);

        return dataset;
    }

}
