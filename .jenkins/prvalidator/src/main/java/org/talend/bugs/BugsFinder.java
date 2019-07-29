package org.talend.bugs;


import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.SAXBugCollectionHandler;
import edu.umd.cs.findbugs.SortedBugCollection;
import edu.umd.cs.findbugs.classfile.UncheckedAnalysisException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BugsFinder {

    public Collection<BugInstance> findAll(Path baseRep) throws IOException {

        final List<BugInstance> bugs = new ArrayList<>();
        Files.walk(baseRep)
                .filter((Path p) -> p.toFile().isFile() && "spotbugsXml.xml".equals(p.toFile().getName()))
                .map(this::findInFiles)
                .forEach(bugs::addAll);

        return bugs;
    }

    private Collection<BugInstance> findInFiles(Path xmlSpotBugFile) {
        SortedBugCollection bg = new SortedBugCollection();

        SAXBugCollectionHandler handler = new SAXBugCollectionHandler(bg, xmlSpotBugFile.toFile());
        XMLReader xr;
        try {
            xr = XMLReaderFactory.createXMLReader();

            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);

            FileReader reader = new FileReader(xmlSpotBugFile.toFile());

            xr.parse(new InputSource(reader));

            return bg.getCollection();
        } catch (SAXException exXml) {
            throw new UncheckedAnalysisException("Sax error ", exXml);
        } catch (IOException exIO) {
            throw new UncheckedIOException("IO error ", exIO);
        }
    }
}
