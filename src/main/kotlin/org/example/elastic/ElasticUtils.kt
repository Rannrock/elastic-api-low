package org.example.elastic

import com.google.gson.Gson
import com.google.gson.GsonBuilder

object ElasticUtils {

    private val gson: Gson = GsonBuilder().setPrettyPrinting().create()

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

    fun generateMapping(map: Map<String, String>): String {

        val propertiesMap: MutableMap<String, Any> = mutableMapOf()

        // Convert the input map into the expected JSON format
        for ((key, value) in map) {
            propertiesMap[key] = mapOf(Pair("type", value))
        }

        val outputMap = mapOf(Pair("mappings",
            mapOf(Pair("properties", propertiesMap))
        ))

        return gson.toJson(outputMap)
    }
}