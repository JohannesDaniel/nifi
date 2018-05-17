package org.apache.nifi.processors.solr;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.solr.SolrClientService;
import org.apache.nifi.solr.SolrClientServiceImpl;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Test;

import java.io.IOException;

public class TestSolrClientTestProcessor {
    @Test
    public void test() throws IOException, SolrServerException, InitializationException {

        TestRunner runner = TestRunners.newTestRunner(SolrClientTestProcessor.class);

        SolrClientServiceImpl service = new SolrClientServiceImpl();
        runner.addControllerService("client", service);
        runner.enableControllerService(service);
        runner.setProperty(SolrClientTestProcessor.SOLR_CLIENT_SERVICE, "client");

        /*
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(SolrUtils.RECORD_WRITER, "writer");

         */

        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(SolrClientTestProcessor.SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(SolrClientTestProcessor.SUCCESS).get(0);

        System.out.println(flowFile.getAttributes());
        System.out.println(new String(runner.getContentAsByteArray(flowFile)));
    }
}
