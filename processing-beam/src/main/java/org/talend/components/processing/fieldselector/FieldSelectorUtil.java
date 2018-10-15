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
package org.talend.components.processing.fieldselector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.talend.components.processing.ProcessingErrorCode;
import scala.collection.JavaConversions;
import scala.util.Try;
import wandou.avpath.Evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class FieldSelectorUtil {

    /**
     * Looking for one of these pattern: [*] [:-18] [:18] [-18:] [18:] [-18:-17] [18:19]
     */
    static Pattern pattern = Pattern.compile("\\[(\\*|:-?\\d+|-?\\d+:|-?\\d+:-?\\d+)\\]");

    /**
     * Extract the values from a list of {@code Evaluator.Ctx} and return them.
     * 
     * @param avPathContexts the result of a AVPath "select" request
     * @param path the AVPath to the element
     * @return the values contained inside the input
     */
    static Object extractValuesFromContext(List<Evaluator.Ctx> avPathContexts, String path) {
        if (avPathContexts.size() > 1 || FieldSelectorUtil.canRetrieveMultipleElements(path)) {
            // Add fields as list
            List<Object> retrievedValues = new ArrayList<>();
            for (Evaluator.Ctx avPathcontext : avPathContexts) {
                retrievedValues.add(avPathcontext.value());
            }
            return retrievedValues;
        } else {
            return avPathContexts.get(0).value();
        }

    }

    /**
     * Generate an indexed record from a given {@code Schema} and its associated data as a map.
     * 
     * @param fields the fields that should be inside the generated indexed record
     * @param schema the schema of the indexed record
     * @return an indexed record
     */
    static IndexedRecord generateIndexedRecord(Map<String, Object> fields, Schema schema) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        for (Entry<String, Object> field : fields.entrySet()) {
            recordBuilder.set(field.getKey(), field.getValue());
        }
        return recordBuilder.build();

    }

    /**
     * check if an AVPath will retrieve multiples data. This can happen when the AVPath describe in its request a call
     * to an array, a predicate or a deep location
     * 
     * @param path the AVPath to check
     * @return true if the AVPath contains mention to an array, a predicate or a deep location
     */
    static boolean canRetrieveMultipleElements(String path) {
        Matcher matcher = pattern.matcher(path);
        return path.contains("{") || path.contains("..") || matcher.find();
    }

    /**
     * Create a generic AVPath that will reach elements without restrictions (predicate/lists) in order to retrieve the
     * schema.
     * 
     * @param path the AVPath to check
     * @return true if the AVPath contains mention to an array, a predicate or a deep location
     */
    static String changeAVPathToSchemaRetriever(String path) {
        return path.replaceAll("\\[[^\\]]*\\]", "\\[\\*]").replaceAll("\\{[^\\}]*\\}", "");
    }

    /**
     * Use an AVPath to extract data from an indexed record
     * 
     * @param record an indexed record
     * @param avPath the path to elements to extract (can be one or multiples elements)
     * @return the extracted data as a list.
     */
    static List<Evaluator.Ctx> getInputFields(IndexedRecord record, String avPath) {
        // Adapt non-avpath syntax to avpath.
        // TODO: This should probably not be automatic, use the actual syntax.
        if (!avPath.startsWith("."))
            avPath = "." + avPath;
        Try<scala.collection.immutable.List<Evaluator.Ctx>> result = wandou.avpath.package$.MODULE$.select(record, avPath);
        if (result.isSuccess()) {
            return new ArrayList<>(JavaConversions.asJavaCollection(result.get()));
        } else {
            // Evaluating the expression failed, and we can handle the exception.
            throw ProcessingErrorCode.createAvpathSyntaxError(result.failed().get(), avPath, -1);
        }
    }

}
