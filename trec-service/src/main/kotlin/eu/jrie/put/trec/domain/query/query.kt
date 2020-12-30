package eu.jrie.put.trec.domain.query

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.infra.xmlMapper
import java.io.File

data class Topics (
        val data: Sequence<Topic>
)

data class Topic (
        val id: Int,
        val disease: String,
        val gene: String,
        val treatment: String
)

class TopicRepository {
        private val topics: List<Topic> = xmlMapper.readValue<Topics>(File("/topics/topics2020.xml"))
                .data
                .toList()

        fun get(id: Int) = topics.find { it.id == id }!!
        fun getAll() = topics.toList()
}
