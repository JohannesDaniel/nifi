package org.apache.nifi.processors.solr;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.solr.SolrJClient;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestSolrClientTestProcessor {
    @Test
    public void test() throws InitializationException {

        TestRunner runner = TestRunners.newTestRunner(SolrClientTestProcessor.class);

        SolrJClient service = new SolrJClient();
        runner.addControllerService("client", service);
        runner.enableControllerService(service);
        runner.setProperty(SolrClientTestProcessor.SOLR_CLIENT_SERVICE, "client");

        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(SolrClientTestProcessor.SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(SolrClientTestProcessor.SUCCESS).get(0);

        System.out.println(flowFile.getAttributes());
        System.out.println(new String(runner.getContentAsByteArray(flowFile)));
    }
}
