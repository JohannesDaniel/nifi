package org.apache.nifi.xml;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Tags({"json", "resultset", "writer", "serialize", "record", "recordset", "row"})
@CapabilityDescription("Writes the results of a RecordSet as a JSON Array. Even if the RecordSet "
        + "consists of a single row, it will be written as an array with a single element.")
public class XMLRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    // suppress nulls
    // pretty print

    // option how to treat arrays
    // option to define name of root?

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        //properties.add(PRETTY_PRINT_JSON);
        //properties.add(SUPPRESS_NULLS);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {

    }


    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) throws SchemaNotFoundException, IOException {
        return null;
    }



}
