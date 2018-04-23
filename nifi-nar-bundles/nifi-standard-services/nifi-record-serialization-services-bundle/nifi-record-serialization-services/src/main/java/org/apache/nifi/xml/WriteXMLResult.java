package org.apache.nifi.xml;

import org.apache.nifi.json.NullSuppression;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class WriteXMLResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {

    final ComponentLog logger;
    final RecordSchema recordSchema;
    final SchemaAccessWriter schemaAccess;

    public WriteXMLResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
                          final NullSuppression nullSuppression, final String dateFormat, final String timeFormat, final String timestampFormat) {

        super(out);

        this.logger = logger;
        this.recordSchema = recordSchema;
        this.schemaAccess = schemaAccess;


    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        final OutputStream out = getOutputStream();
        schemaAccess.writeHeader(recordSchema, out);

        // write root tag ?

    }
    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {

        // closing root tag?

        return schemaAccess.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {

        // any writer that has to be closed?

        super.close();
    }

    @Override
    protected Map<String, String> writeRecord(Record record) throws IOException {
        return null;
    }

    @Override
    public String getMimeType() {
        return null;
    }

    @Override
    public WriteResult writeRawRecord(Record record) throws IOException {
        return null;
    }
}
