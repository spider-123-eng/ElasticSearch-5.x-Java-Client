package com.es.service;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * @author revanthreddy
 *
 */
public class DataService {

	Client client;

	public DataService(Client client) {
		this.client = client;
	}

	public void multiGet() {
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("twitter", "tweet", "1")
				.add("twitter", "tweet", "2", "3", "4")
				// .add("another", "type", "foo")
				.get();
		for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
			GetResponse response = itemResponse.getResponse();
			if (response.isExists()) {
				String jsonRes = response.getSourceAsString();
				System.out.println(jsonRes);
			}
		}
	}

	public List<String> getMatchAllQueryData() {
		QueryBuilder query = matchAllQuery();
		//System.out.println("getMatchAllQueryCount query =>" + query.toString());
		SearchHit[] hits = client.prepareSearch("test").setQuery(query).execute().actionGet().getHits().getHits();
		List<String> list = new ArrayList<String>();
		for (SearchHit hit : hits) {
			// hit.sourceAsMap()
			list.add(hit.getSourceAsString());
		}
		return list;
	}

	public List<String> getBoolQueryData() {
		QueryBuilder query = boolQuery().must(termQuery("name", "revanth")).must(termQuery("location", "india"));
		//System.out.println("getBoolQueryCount query =>" + query.toString());
		SearchHit[] hits = client.prepareSearch("test").setQuery(query).execute().actionGet().getHits().getHits();
		List<String> list = new ArrayList<String>();
		for (SearchHit hit : hits) {
			// hit.sourceAsMap()
			list.add(hit.getSourceAsString());
		}
		return list;
	}

	public List<String> getPhraseQueryData() {
		QueryBuilder query = matchPhraseQuery("name", "revanth");
		//System.out.println("getPhraseQueryCount query =>" + query.toString());
		SearchHit[] hits = client.prepareSearch("test").setQuery(query).execute().actionGet().getHits().getHits();
		List<String> list = new ArrayList<String>();
		for (SearchHit hit : hits) {
			// hit.sourceAsMap()
			list.add(hit.getSourceAsString());
		}
		return list;
	}

}
