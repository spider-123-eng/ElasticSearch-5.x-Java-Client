package com.es.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

/**
 * @author revanthreddy
 *
 */
public class DeleteService {

	Client client;

	public DeleteService(Client client) {
		this.client = client;
	}

	public void delete(String id) {
		client.prepareDelete("twitter", "tweet", id).get();
	}

	public long deleteByMatchQuery(String name) {

		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery("user", name))
				//.filter(QueryBuilders.typeQuery("tweet"))
				.source("twitter").get();
		return response.getDeleted();
	}

	public long deleteByTermQuery(String name) {

		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.termQuery("user", name))
				//.filter(QueryBuilders.typeQuery("tweet"))
				.source("twitter").get();
		return response.getDeleted();
	}
}
