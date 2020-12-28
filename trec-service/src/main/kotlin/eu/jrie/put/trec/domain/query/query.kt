package eu.jrie.put.trec.domain.query

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.infra.xmlMapper
import java.io.File

data class Queries (
        val data: Sequence<Query>
)

data class Query (
        val id: Int,
        val disease: String,
        val gene: String,
        val treatment: String
)

class QueryRepository {
        private val queries: List<Query> = xmlMapper.readValue<Queries>(File("/topics/topics2020.xml"))
                .data
                .toList()

        fun get(id: Int): Query = queries.find { it.id == id }!!
}
