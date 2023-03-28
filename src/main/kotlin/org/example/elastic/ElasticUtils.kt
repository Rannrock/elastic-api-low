package org.example.elastic

import com.google.gson.Gson
import com.google.gson.GsonBuilder

object ElasticUtils {

    private val gson: Gson = GsonBuilder().create()

    /**
     * Determines if a given Elasticsearch index name is valid.
     * An Elasticsearch index name is considered valid if it is not empty, contains only
     * lowercase letters, digits, specific special chars, and starts with a letter.
     *
     * Authorize special chars : '-', '_', '+', '.'
     *
     * This function does not check uppercase letters. It is the responsibility of caller to send a lowercase string.
     */
    fun isIndexNameValid(name: String): Boolean {

        val forbiddenChar: List<Char> = listOf('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', ':', '#')
        val forbiddenFirstChar: List<Char> = listOf('-', '_', '+', '.')

        if (name == "." || name == "..") {
            return false
        }

        if (name.toByteArray().size > 255) {
            return false
        }

        if (forbiddenFirstChar.any { firstChar -> name.startsWith(firstChar) }) {
            return false
        }

        if (forbiddenChar.any { char -> name.contains(char) }) {
            return false
        }

        return true
    }

    /**
     * Generates a JSON string representing the mapping for an Elasticsearch index.
     *
     * @param map a map of field names to field types for the index mapping
     * @return a JSON string representing the mapping for the index
     */
    fun generateMapping(map: Map<String, String>): String {

        val propertiesMap: MutableMap<String, Any> = mutableMapOf()

        for ((key, value) in map) {
            propertiesMap[key] = mapOf(Pair("type", value))
        }

        val outputMap = mapOf(
            Pair(
                "mappings",
                mapOf(
                    Pair("properties", propertiesMap)
                )
        ))

        return gson.toJson(outputMap)
    }

    /**
     * Builds a bulk indexing request body for Elasticsearch.
     *
     * @param documents a list of maps representing the documents to index,
     * where each map represents a single document and maps field names to field values
     * @return a JSON string representing the bulk indexing request body for Elasticsearch
     */
    fun buildBulkBody(documents: List<Map<String, Any>>): String {
        val stringBuilder = StringBuilder()
        for (document in documents) {
            val metadata = "{\"index\":{}}"
            val json = gson.toJson(document)
            stringBuilder.append(metadata).append("\n").append(json).append("\n")
        }
        val test = stringBuilder.toString()

        println(test)
        return test
    }

    /**
     * Splits a list of documents into smaller sublists if the total size of the documents exceeds a specified limit.
     *
     * @param documents the list of documents to split
     * @param maxSize the maximum size in bytes of each sublist
     * @return a list of sublists, each containing up to maxSize bytes of documents
     */
    fun splitDocumentsBySize(documents: List<Map<String, Any>>, maxSize: Int): List<List<Map<String, Any>>> {
        val result = mutableListOf<List<Map<String, Any>>>()
        var currentList = mutableListOf<Map<String, Any>>()
        var currentSize = 0

        for (document in documents) {
            val json = gson.toJson(document)
            val documentSize = json.toByteArray().size

            if (currentSize + documentSize > maxSize) {
                // If adding this document to the current list would exceed the size limit,
                // start a new list and add the current document to it
                result.add(currentList)
                currentList = mutableListOf(document)
                currentSize = documentSize
            } else {
                // Otherwise, add the current document to the current list
                currentList.add(document)
                currentSize += documentSize
            }
        }

        if (currentList.isNotEmpty()) {
            // Add any remaining documents to the result list
            result.add(currentList)
        }

        return result
    }
}