package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class TestXMLRecordReader {

    @Test
    public void test() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/cds.xml");
        XMLRecordReader rr = new XMLRecordReader(is);
        rr.nextRecord(true, true);



    }

}
