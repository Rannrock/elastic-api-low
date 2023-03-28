package org.example.elastic

object Main {
    @JvmStatic
    fun main(args: Array<String>) {

        val host = "192.168.0.200"
        val port = 9200

        val elastic2 = ElasticClient(host, port)

        val indiceName = "company-test-07"

        val inputMap: Map<String, String> = mapOf(
            "field1" to "text",
            "field2" to "long",
            "field3" to "date"
        )

        val data1 = mapOf("field1" to "foo", "field2" to 64, "field3" to "2023-03-28")
        val data2 = mapOf("field1" to "bar", "field2" to 1000, "field3" to "2023-02-12")

        val data: List<Map<String, Any>> = listOf(data1, data2)

        println(ElasticUtils.generateMapping(inputMap))

        println("start")

        elastic2.createIndexAsync(indiceName, inputMap) { x, e -> if (x) {println(x)} else {println(e)}  }

        val documents: List<Map<String, Any>> = listOf(data1, data2)
        val maxSize = 100 * 1024 * 1024 // 100 MB
        val documentSublists = ElasticUtils.splitDocumentsBySize(documents, maxSize)

        for (sublist in documentSublists) {
            elastic2.bulkIndex(indiceName, sublist)
        }

        println("end")

    }

}