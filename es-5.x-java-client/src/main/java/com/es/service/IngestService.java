package com.es.service;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * @author revanthreddy
 *
 */

public class IngestService {

	Client client;

	public IngestService(Client client) {
		this.client = client;
	}

	public void ingest(String type, String doc) {
		client.prepareIndex("twitter", "tweet").setSource(doc, XContentType.JSON).get();
	}

	public boolean ingest(String type, List<String> docs) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		docs.forEach(doc -> bulkRequest.add(client.prepareIndex("twitter", type).setSource(doc, XContentType.JSON)));
		return bulkRequest.get().hasFailures();
	}

	public boolean ingest(String index, String type, List<String> docs) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		docs.forEach(doc -> bulkRequest.add(client.prepareIndex(index, type).setSource(doc, XContentType.JSON)));
		return bulkRequest.get().hasFailures();
	}

}
