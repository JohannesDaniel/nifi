package org.apache.nifi.solr;

import org.apache.nifi.controller.ControllerService;

import java.io.IOException;

public interface SolrClientService extends ControllerService {
// change descriptions

    /**
     * Perform a search using the JSON DSL.
     * @param query A JSON string reprensenting the query.
     * @param index The index to target. Optional.
     * @param type The type to target. Optional. Will not be used in future versions of ElasticSearch.
     * @return A SearchResponse object if successful.
     */
    String search(String query, String index, String type) throws IOException;

    /**
     * Build a transit URL to use with the provenance reporter.
     * @param index Index targeted. Optional.
     * @param type Type targeted. Optional
     * @return a URL describing the ElasticSearch cluster.
     */
    String getTransitUrl(String index, String type);
}
