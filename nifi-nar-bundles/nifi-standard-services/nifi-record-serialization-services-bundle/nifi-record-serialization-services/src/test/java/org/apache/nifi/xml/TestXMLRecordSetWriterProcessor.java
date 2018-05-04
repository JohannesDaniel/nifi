package org.apache.nifi.xml;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestXMLRecordSetWriterProcessor extends AbstractProcessor {

    static final PropertyDescriptor XML_WRITER = new PropertyDescriptor.Builder()
            .name("xml_writer")
            .description("xml_writer")
            .identifiesControllerService(XMLRecordSetWriter.class)
            .required(true)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("success").build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        final RecordSetWriterFactory writerFactory = context.getProperty(XML_WRITER).asControllerService(RecordSetWriterFactory.class);
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                try {

                    final RecordSchema schema = writerFactory.getSchema(null, null);

                    RecordSet recordSet = getRecordSet();

                    final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out);


                    writer.write(recordSet);
                    writer.flush();


                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        session.transfer(flowFile, SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<PropertyDescriptor>() {{ add(XML_WRITER); }};
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{ add(SUCCESS); }};
    }

    protected static RecordSet getRecordSet() {
        Object[] arrayVals = {1, null, 3};

        Map<String,Object> recordFields = new HashMap<>();
        recordFields.put("name1", "val1");
        recordFields.put("name2", null);
        recordFields.put("array_field", arrayVals);

        RecordSchema emptySchema = new SimpleRecordSchema(Collections.emptyList());

        List<Record> records = new ArrayList<>();
        records.add(new MapRecord(emptySchema, recordFields));
        records.add(new MapRecord(emptySchema, recordFields));

        return new ListRecordSet(emptySchema, records);
    }



}
