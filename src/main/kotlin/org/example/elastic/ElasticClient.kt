package org.example.elastic

import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException

/**
 * A client for interacting with Elasticsearch.
 * This class provides methods for creating and managing Elasticsearch indexes, as well as indexing documents in bulk.
 *
 * @property host the hostname or IP address of the Elasticsearch server
 * @property port the port number on which the Elasticsearch server is listening
 */
class ElasticClient(private val host: String, private val port: Int) {

    private val httpClient = OkHttpClient()

    /**
     * Creates a new Elasticsearch index with the given name and mapping.
     *
     * @param name the name of the index to create
     * @param mapping a map of field names to field types for the index mapping
     * @return true if the index was created successfully, false otherwise
     * @throws IllegalArgumentException if the given index name is invalid
     */
    fun createIndex(name: String, mapping: Map<String, String>): Boolean {
        if (!isValidIndexName(name)) {
            throw IllegalArgumentException("Invalid index name: $name")
        }

        val requestBody = ElasticUtils.generateMapping(mapping).toRequestBody("application/json; charset=utf-8".toMediaType())
        val request = Request.Builder()
            .url("http://$host:$port/$name")
            .put(requestBody)
            .build()

        val response = httpClient.newCall(request).execute()

        println(response.message)
        return response.isSuccessful
    }

    /**
     * Asynchronously creates a new Elasticsearch index with the given name and mapping.
     *
     * @param name the name of the index to create
     * @param mapping a map of field names to field types for the index mapping
     * @param callback a callback function to invoke when the index creation is complete
     * @throws IllegalArgumentException if the given index name is invalid
     */
    fun createIndexAsync(name: String, mapping: Map<String, String>, callback: (Boolean, Exception?) -> Unit) {
        if (!isValidIndexName(name)) {
            callback(false, IllegalArgumentException("Invalid index name: $name"))
            return
        }

        val requestBody = ElasticUtils.generateMapping(mapping).toRequestBody("application/json; charset=utf-8".toMediaType())
        val request = Request.Builder()
            .url("http://$host:$port/$name")
            .put(requestBody)
            .build()

        httpClient.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                val success = response.isSuccessful
                if (!success) {
                    callback(false, Exception("Failed to create index"))
                } else {
                    callback(true, null)
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                callback(false, e)
            }
        })
    }

    /**
     * Indexes a list of documents in bulk for the specified index.
     *
     * @param name the name of the index to bulk index documents for
     * @param documents a list of maps representing the documents to index, where each map represents a single document and maps field names to field values
     * @return true if the documents were indexed successfully, false otherwise
     * @throws IllegalArgumentException if the index name is not valid
     */
    fun bulkIndex(name: String, documents: List<Map<String, Any>>): Boolean {
        if (!isValidIndexName(name)) {
            throw IllegalArgumentException("Invalid index name: $name")
        }

        val bulkRequestBody = ElasticUtils.buildBulkBody(documents).toRequestBody("application/json; charset=utf-8".toMediaType())
        val request = Request.Builder()
            .url("http://$host:$port/$name/_bulk")
            .post(bulkRequestBody)
            .build()

        val response = httpClient.newCall(request).execute()

        print("bulk response: ")
        println(response.message)
        return response.isSuccessful
    }

    /**
     * Indexes a list of documents in bulk for the specified index asynchronously.
     *
     * @param name the name of the index to bulk index documents for
     * @param documents a list of maps representing the documents to index, where each map represents a single document and maps field names to field values
     * @param callback a callback function that will be invoked with the result of the operation
     *        when it completes. The first argument of the callback indicates whether the operation
     *        was successful, and the second argument (if any) is an exception that was thrown during
     *        the operation (or null if no exception was thrown).
     */
    fun bulkIndexAsync(name: String, documents: List<Map<String, Any>>, callback: (Boolean, Exception?) -> Unit) {
        if (!isValidIndexName(name)) {
            callback(false, IllegalArgumentException("Invalid index name: $name"))
            return
        }

        val documentSublists = ElasticUtils.splitDocumentsBySize(documents, 100 * 1024 * 1024) // Split documents into sublists of at most 100 MB

        var currentSublistIndex = 0

        val requestBuilder = Request.Builder()
            .url("http://$host:$port/$name/_bulk")

        val requestCallback = object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                callback(false, e)
            }

            override fun onResponse(call: Call, response: Response) {
                if (!response.isSuccessful) {
                    callback(false, Exception("Failed to index documents"))
                    return
                }

                if (currentSublistIndex < documentSublists.size) {
                    // If there are more sublists to index, create a new request with the next sublist and send it
                    val sublist = documentSublists[currentSublistIndex]
                    val bulkRequestBody = ElasticUtils.buildBulkBody(sublist).toRequestBody("application/json; charset=utf-8".toMediaType())
                    val request = requestBuilder.post(bulkRequestBody).build()
                    currentSublistIndex++
                    httpClient.newCall(request).enqueue(this)
                } else {
                    // Otherwise, all sublists have been indexed successfully
                    callback(true, null)
                }
            }
        }

        // Start by creating a request with the first sublist of documents and sending it
        val sublist = documentSublists[currentSublistIndex]
        val bulkRequestBody = ElasticUtils.buildBulkBody(sublist).toRequestBody("application/json; charset=utf-8".toMediaType())
        val request = requestBuilder.post(bulkRequestBody).build()
        currentSublistIndex++
        httpClient.newCall(request).enqueue(requestCallback)
    }


    /**
     * Checks if an Elasticsearch index with the given name exists.
     *
     * @param name the name of the index to check
     * @return true if the index exists, false otherwise
     */
    fun indexExists(name: String): Boolean {
        val request = Request.Builder()
            .url("$host:$port/$name")
            .head()
            .build()

        val response = httpClient.newCall(request).execute()
        return response.isSuccessful
    }

    private fun isValidIndexName(name: String): Boolean {
        val pattern = Regex("^[a-zA-Z][a-zA-Z0-9_-]*\$")
        return pattern.matches(name)
    }
}