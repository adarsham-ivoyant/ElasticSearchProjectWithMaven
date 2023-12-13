package org.ivoyant;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class App {
    public void function(){

    }
    public static void main(String[] args) {


        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")))) {
            CRUDOperations crudOperations = new Operations(client);

            String index = "sample_db";
            String documentId = "document_id";


            Map<String, Object> document = new HashMap<>();
            document.put("name", "Virat");
            document.put("lastname", "Kholi");
            document.put("age", 35);
            document.put("city", "Banglore");
            crudOperations.indexDocument(index, documentId, document);



            // Get a document
            Map<String, Object> retrievedDocument = crudOperations.getDocument(index, documentId);
            System.out.println("Retrieved Document: " + retrievedDocument);

            // Get all documents
            Map<String, Object> allDocuments = crudOperations.getAllDocuments(index);

            // Print all retrieved documents
            for (Map.Entry<String, Object> entry : allDocuments.entrySet()) {
                System.out.println("Document ID: " + entry.getKey() + ", Document: " + entry.getValue());
            }

            // Update a document
            Map<String, Object> updatedDocument = new HashMap<>();
            updatedDocument.put("name", "vvv");
            crudOperations.updateDocument(index, documentId, updatedDocument);

            // Delete a document
            //  crudOperations.deleteDocument(index, documentId);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
