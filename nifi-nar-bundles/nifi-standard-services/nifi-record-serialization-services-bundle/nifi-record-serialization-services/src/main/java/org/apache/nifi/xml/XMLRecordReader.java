package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;

public class XMLRecordReader implements RecordReader {

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {


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
