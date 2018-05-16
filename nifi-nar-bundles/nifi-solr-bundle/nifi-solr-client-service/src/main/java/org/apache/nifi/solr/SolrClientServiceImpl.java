package org.apache.nifi.solr;

import org.apache.nifi.controller.AbstractControllerService;

import java.io.IOException;

public class SolrClientServiceImpl extends AbstractControllerService implements SolrClientService {


    @Override
    public String search(String query, String index, String type) throws IOException {
        return "SEARCH";
    }

    @Override
    public String getTransitUrl(String index, String type) {
        return "URI";
    }
}
