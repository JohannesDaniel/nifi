package org.apache.nifi.processors.solr;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.solr.SolrClientService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SolrClientTestProcessor extends AbstractProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").build();

    public static final PropertyDescriptor SOLR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("solr_client_service")
            .identifiesControllerService(SolrClientService.class)
            .required(true)
            .build();



    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_CLIENT_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        final SolrClientService solrClientService = context.getProperty(SOLR_CLIENT_SERVICE).asControllerService(SolrClientService.class);
        Map<String,String> map = new HashMap<>();
        try {
            map.put("transituri", solrClientService.getTransitUrl("", ""));
            map.put("search",  solrClientService.search("", "", ""));
        } catch (IOException e) {
            e.printStackTrace();
        }
        flowFile = session.putAllAttributes(flowFile, map);


        flowFile = session.write(flowFile, out -> {
            out.write("results".getBytes());
        });

        session.transfer(flowFile, SUCCESS);

    }

}
