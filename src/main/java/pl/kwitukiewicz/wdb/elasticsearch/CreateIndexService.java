package pl.kwitukiewicz.wdb.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Krzysztof Witukiewicz
 */
public class CreateIndexService {
	
	public static final String INDEX = "dewiktionary_dump";
	public static final String TYPE = "page";
	
	private static final Logger logger = LoggerFactory.getLogger(CreateIndexService.class);
	private static final Logger errorLogger = LoggerFactory.getLogger("indexation_errors");
	
	private final RestClient client;
	private final ObjectMapper objectMapper;
	
	private final AtomicInteger indexationCounter = new AtomicInteger();
	private final Map<String, Exception> indexationExceptions = new ConcurrentHashMap<>();
	
	private CreateIndexService() {
		client = RestClient.builder(new HttpHost("localhost", 9200, "http"))
		                   .setHttpClientConfigCallback(httpClientBuilder -> {
			                   CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			                   credentialsProvider.setCredentials(AuthScope.ANY,
			                                                      new UsernamePasswordCredentials("elastic",
			                                                                                      "changeme"));
			                   return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		                   })
		                   .build();
		objectMapper = new ObjectMapper();
	}
	
	public static CreateIndexService create() {
		CreateIndexService service = new CreateIndexService();
		service.initialize();
		return service;
	}
	
	public void indexWiktionaryPage(WiktionaryPageDocument document) {
		String id;
		try {
			id = URLEncoder.encode(document.getNamespace() + ":" + document.getTitle(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Error indexing " + document, e);
		}
		String endpoint = String.format("/%s/%s/%s", INDEX, TYPE, id);
		byte[] body;
		try {
			body = objectMapper.writeValueAsBytes(document);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		ByteArrayEntity entity = new ByteArrayEntity(body);
		client.performRequestAsync("PUT", endpoint, Collections.emptyMap(), entity,
		                           new ResponseListener() {
			                           @Override
			                           public void onSuccess(Response response) {
				                           indexationCounter.incrementAndGet();
			                           }
			
			                           @Override
			                           public void onFailure(Exception exception) {
				                           indexationExceptions.put(id, exception);
			                           }
		                           },
		                           new BasicHeader("Content-Type", ContentType.APPLICATION_JSON.toString()));
	}
	
	public void cleanup() {
		setRefreshInterval("1s");
		logger.info("The indexation was finished! Successfully indexed {} documents. There were {} errors!",
		            indexationCounter.get(), indexationExceptions.size());
		if (!indexationExceptions.isEmpty()) {
			errorLogger.error("There were {} errors during indexation! The errors are:", indexationExceptions.size());
			indexationExceptions.forEach((id, e) -> errorLogger.error("Exception indexing document id=" + id, e));
		}
		try {
			client.close();
		} catch (IOException e) {
			logger.error("Error closing client!", e);
		}
	}
	
	private void initialize() {
		deleteIndex();
		createIndex();
	}
	
	private void deleteIndex() {
		try {
			client.performRequest("DELETE", "/" + INDEX);
		} catch (ResponseException e) {
			if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
				logger.info("Index {} does not exist - nothing to delete.", INDEX);
			} else {
				throw new RuntimeException("Unexpected error deleting index " + INDEX, e);
			}
		} catch (IOException e) {
			throw new RuntimeException("Unexpected error deleting index " + INDEX, e);
		}
	}
	
	private void createIndex() {
		String jsonString = "{\"settings\":{\"index\": {\"number_of_shards\": \"1\", \"number_of_replicas\": \"0\"}}}";
		HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
		try {
			client.performRequest("PUT", INDEX, Collections.emptyMap(), entity);
		} catch (Exception e) {
			throw new RuntimeException("Error creating index " + INDEX, e);
		}
		setRefreshInterval("0");
	}
	
	private void setRefreshInterval(String value) {
		String jsonString = String.format("{\"index\":{\"refresh_interval\":\"%s\"}}", value);
		HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
		try {
			client.performRequest("PUT", INDEX + "/_settings", Collections.emptyMap(), entity);
		} catch (Exception e) {
			logger.error("Error setting refresh_interval to " + value, e);
		}
	}
}
