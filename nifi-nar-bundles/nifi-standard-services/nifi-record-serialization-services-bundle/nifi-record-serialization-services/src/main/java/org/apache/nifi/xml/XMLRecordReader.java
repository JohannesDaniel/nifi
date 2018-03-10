package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class XMLRecordReader implements RecordReader {

    // alles auf final

    private InputStream in;
    private RecordSchema schema;
    private Tuple<String, String> xmlRecordPathElements;

    private XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    private XMLEventReader xmlEventReader;

    public XMLRecordReader(InputStream in, RecordSchema schema, Tuple<String, String> xmlRecordPathElements) throws MalformedRecordException {
        this.in =  in;
        this.schema = schema;
        this.xmlRecordPathElements = xmlRecordPathElements;

        try {
            xmlEventReader = xmlInputFactory.createXMLEventReader(in);
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }

    }


    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {



            while(xmlEventReader.hasNext()){
                XMLEvent xmlEvent = xmlEventReader.nextEvent();
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    System.out.println(startElement.getName());

                }



            }




        } catch (XMLStreamException e) {
            e.printStackTrace();
        }


        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
