package com.es.util;

import java.net.InetAddress;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @author revanthreddy
 *
 */
public class ESTransportClient {
	private final static Logger logger = Logger.getLogger(ESTransportClient.class);

	public Optional<Client> getClient(String clusterName, String host, int port) {
		try {
			Settings settings = Settings.builder().put("cluster.name", clusterName).put("client.transport.sniff", true)
					.build();
			TransportClient client = new PreBuiltTransportClient(settings);
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			return Optional.of(client);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error while initializing TransportClient :" + e.getMessage(), e);
			return Optional.empty();
			
		}
	}

}
