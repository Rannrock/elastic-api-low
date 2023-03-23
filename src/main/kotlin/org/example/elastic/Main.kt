package org.example.elastic

object Main {
    @JvmStatic
    fun main(args: Array<String>) {

        val elastic = Elastic("192.168.0.200", 9200, "http")

        val indiceName: String = "company-test-04"

        val inputMap: Map<String, String> = mapOf(
            "field1" to "text",
            "field2" to "long",
            "field3" to "date"
        )

        println(ElasticUtils.isIndexNameValid(indiceName))

        elastic.createIndice(indiceName, inputMap)

        elastic.closeClient()
    }
}