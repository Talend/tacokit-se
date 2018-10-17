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
package org.talend.components.processing.python;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.python.antlr.AnalyzingParser;
import org.python.antlr.PythonTree;
import org.python.antlr.Visitor;
import org.python.antlr.ast.Import;
import org.python.antlr.ast.ImportFrom;
import org.python.antlr.ast.alias;
import org.python.antlr.base.mod;
import org.python.antlr.runtime.ANTLRStringStream;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.core.PyUnicode;
import org.python.util.PythonInterpreter;
import org.talend.components.processing.ProcessingErrorCode;
import org.talend.daikon.avro.converter.JsonGenericRecordConverter;
import org.talend.daikon.avro.inferrer.JsonSchemaInferrer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PythonDoFn extends DoFn<IndexedRecord, IndexedRecord> {

    private PythonConfiguration configuration = null;

    private PythonInterpreter interpreter = null;

    private PyObject pyFn = null;

    private JsonGenericRecordConverter jsonGenericRecordConverter = null;

    private List<String> moduleBlacklist = Arrays.asList("os", "signal", "java.security",
            // This is absolutely crucial for these two to be in the blacklist
            "java.security.SecureClassLoader", "java.security.Permission");

    PythonDoFn withConfiguration(PythonConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    @Setup
    public void setup() throws Exception {
        interpreter = new PythonInterpreter();
        if (PythonConfiguration.MapType.MAP.equals(configuration.getMapType())) {
            String userData = setUpMap();
            checkImportBlacklist(userData);
            interpreter.exec(userData);
        } else { // flatmap
            String userData = setUpFlatMap();
            checkImportBlacklist(userData);
            interpreter.exec(userData);
        }
        pyFn = interpreter.get("userFunction");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        if (context.element() != null) {
            if (PythonConfiguration.MapType.MAP.equals(configuration.getMapType())) {
                map(context.element(), context);
            } else { // flatmap
                flatMap(context.element(), context);
            }
        }
    }

    private void checkImportBlacklist(String userData) throws Exception {
        mod tree = new AnalyzingParser(new ANTLRStringStream(userData), "", "UTF-8").parseModule();
        Visitor vis = new Visitor() {

            @Override
            public Object visitImport(Import node) throws Exception {
                for (alias importName : node.getInternalNames()) {
                    for (String blackListElement : moduleBlacklist) {
                        if (importName.getInternalName().equals(blackListElement)
                                || importName.getInternalName().startsWith(blackListElement + ".")) {
                            throw ProcessingErrorCode.createInvalidPythonImportErrorException(importName.getInternalName());
                        }

                    }
                }
                return node;
            }

            @Override
            public Object visitImportFrom(ImportFrom node) throws Exception {
                for (String blackListElement : moduleBlacklist) {
                    if (node.getInternalModule().equals(blackListElement)
                            || node.getInternalModule().startsWith(blackListElement + ".")) {
                        throw ProcessingErrorCode.createInvalidPythonImportErrorException(node.getInternalModule());
                    }
                }

                for (alias importName : node.getInternalNames()) {
                    for (String blackListElement : moduleBlacklist) {
                        String fullImport = node.getInternalModule() + "." + importName.getInternalName();
                        if (fullImport.equals(blackListElement) || fullImport.startsWith(blackListElement + ".")) {
                            throw ProcessingErrorCode.createInvalidPythonImportErrorException(fullImport);
                        }
                    }
                }
                return node;
            }
        };
        for (PythonTree c : tree.getChildren()) {
            try {
                vis.visit(c);
            } catch (Exception e) {
                // error case, we kill the interpreter to avoid caching mechanism.
                interpreter.close();
                interpreter = null;
                throw e;
            }
        }

    }

    private String setUpMap() {
        return "import json\n" //
                + "import collections\n" //
                + "def userFunction(inputJSON):\n" //
                + "  input = json.loads(inputJSON, object_pairs_hook=collections.OrderedDict)\n" //
                + "  output = json.loads(\"{}\", object_pairs_hook=collections.OrderedDict)\n" //
                + "  try:\n" //
                + "    emptyFunction()\n" //
                + "    " + configuration.getPythonCode().replaceAll("\n", "\n    ").trim() + "\n"//
                + "  except SystemExit:\n" //
                + "    pass\n" //
                + "  return json.dumps(output)\n" //
                + "def emptyFunction():\n" // to avoid compile error when empty user code block
                + "  pass";
    }

    private String setUpFlatMap() {
        return "import json\n" //
                + "import collections\n" //
                + "def userFunction(inputJSON):\n" //
                + "  input = json.loads(inputJSON, object_pairs_hook=collections.OrderedDict)\n" //
                + "  outputList = []\n" //
                + "  try:\n" //
                + "    emptyFunction()\n" //
                + "    " + configuration.getPythonCode().replaceAll("\n", "\n    ").trim() + "\n"//
                + "  except SystemExit:\n" //
                + "    pass\n" //
                + "  return [ json.dumps(outputElement) for outputElement in outputList ]\n" //
                + "def emptyFunction():\n" // to avoid compile error when empty user code block
                + "  pass";
    }

    private void map(IndexedRecord input, ProcessContext context) throws IOException {
        PyObject output = pyFn.__call__(new PyUnicode(input.toString()));

        if (jsonGenericRecordConverter == null) {
            JsonSchemaInferrer jsonSchemaInferrer = new JsonSchemaInferrer(new ObjectMapper());
            Schema jsonSchema = jsonSchemaInferrer.inferSchema(output.toString());
            jsonGenericRecordConverter = new JsonGenericRecordConverter(jsonSchema);
        }

        GenericRecord outputRecord = jsonGenericRecordConverter.convertToAvro(output.toString());
        context.output(outputRecord);
    }

    private void flatMap(IndexedRecord input, ProcessContext context) throws IOException {
        // Prepare Python environment
        PyObject outputList = pyFn.__call__(new PyUnicode(input.toString()));

        if (outputList instanceof PyList) {
            PyList list = (PyList) outputList;
            for (Object output : list) {
                if (jsonGenericRecordConverter == null) {
                    JsonSchemaInferrer jsonSchemaInferrer = new JsonSchemaInferrer(new ObjectMapper());
                    Schema jsonSchema = jsonSchemaInferrer.inferSchema(output.toString());
                    jsonGenericRecordConverter = new JsonGenericRecordConverter(jsonSchema);
                }
                GenericRecord outputRecord = jsonGenericRecordConverter.convertToAvro(output.toString());
                context.output(outputRecord);
            }
        }
    }

    @Teardown
    public void tearDown() {
        interpreter.close();
    }

}