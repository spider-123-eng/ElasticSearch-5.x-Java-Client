package com.es.service;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

/**
 * @author revanthreddy
 *
 */
public class UpdateService {

	Client client;

	public UpdateService(Client client) {
		this.client = client;
	}

	public void bulkUpdate() {

		BulkRequestBuilder bulkRequest = client.prepareBulk();

		try {
			bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
					.setSource(jsonBuilder().startObject().field("user", "arjun").field("postDate", new Date())
							.field("message", "trying out Elasticsearch").endObject()));

			bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
					.setSource(jsonBuilder().startObject().field("user", "arjun").field("postDate", new Date())
							.field("message", "another post").endObject()));

			BulkResponse bulkResponse = bulkRequest.get();
			if (bulkResponse.hasFailures()) {
				// process failures by iterating through each bulk response item
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
