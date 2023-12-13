package org.ivoyant;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Operations implements CRUDOperations {

    private final RestHighLevelClient client;

    public Operations(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public void indexDocument(String index, String id, Map<String, Object> document) throws IOException {
        IndexRequest request = new IndexRequest(index)
                .id(id)
                .source(document, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
    }

    @Override
    public Map<String, Object> getDocument(String index, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        return getResponse.getSourceAsMap();
    }


    @Override
    public Map<String, Object> getAllDocuments(String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Map<String, Object> allDocuments = new HashMap<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String documentId = hit.getId();
            Map<String, Object> document = hit.getSourceAsMap();
            allDocuments.put(documentId, document);
        }

        return allDocuments;
    }


    @Override
    public void updateDocument(String index, String id, Map<String, Object> updatedDocument) throws IOException {
        UpdateRequest request = new UpdateRequest(index, id)
                .doc(updatedDocument, XContentType.JSON);

        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
    }


    @Override
    public void deleteDocument(String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, id);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
    }

    @Override
    public void closeClient() throws IOException {
        client.close();
    }


}
