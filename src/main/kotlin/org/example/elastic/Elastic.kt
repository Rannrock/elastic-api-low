package org.example.elastic

import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.*
import java.io.ByteArrayOutputStream


class Elastic constructor(hostname: String, port: Int, scheme: String){

    private val restClientBuilder: RestClientBuilder = RestClient.builder(
        HttpHost(hostname, port, scheme)
    )

    // TODO: Add basic auth
    private val defaultHeaders: Array<Header> = arrayOf(BasicHeader("Content-Type", "application/json"))
    private val charset = "UTF-8"

    private val restClient: RestClient = restClientBuilder.setDefaultHeaders(defaultHeaders).build()

    private val byteArrayOutputStream: ByteArrayOutputStream = ByteArrayOutputStream()

    /**
     * Check if an index exists.
     * If the client has no permission to see an index, the function returns false.
     */
    fun exist(name: String): Boolean {

        val request = Request(
            "HEAD",
            name
        )
        val response: Response = restClient.performRequest(request)

        if (response.statusLine.statusCode >= 500) {
            throw RuntimeException(response.statusLine.reasonPhrase)
        }

        return response.statusLine.statusCode == 200
    }

    /**
     * Create empty indices without mapping.
     */
    fun createIndice(name: String): Int {

        val request: Request = Request(
            "PUT",
            "/$name"
        )
        val response: Response = restClient.performRequest(request)

        if (response.statusLine.statusCode != 200) {
            throw RuntimeException(response.statusLine.reasonPhrase)
        }

        return response.statusLine.statusCode
    }

    fun createIndice(name: String, mapping: Map<String, String>): Int {

        val request: Request = Request(
            "PUT",
            "/$name"
        )

        request.setJsonEntity(ElasticUtils.generateMapping(mapping))

        val response: Response = restClient.performRequest(request)

        if (response.statusLine.statusCode != 200) {
            throw RuntimeException(response.statusLine.reasonPhrase)
        }

        return response.statusLine.statusCode
    }


    fun getMapping(indiceName: String): String{

        byteArrayOutputStream.reset()
        val request = Request(
            "GET",
            "/$indiceName/_mapping"
        )

        val response: Response = restClient.performRequest(request)

        if (response.statusLine.statusCode != 200) {
            throw RuntimeException(response.statusLine.reasonPhrase)
        }

        return EntityUtils.toString(response.entity, charset)
    }

    /**
     * Closes this elastic client and releases any system resources associated with it
     */
    fun closeClient(): Unit {
        restClient.close()
    }


}