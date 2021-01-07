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

        fun get(id: Int, set: String) = getTopicsFromSet(set).find { it.id == id }!!
        fun getAll(set: String) = getTopicsFromSet(set).toList()

        private fun getTopicsFromSet(set: String) = File("/topics/$set.xml")
                .let { xmlMapper.readValue<Topics>(it) }
                .data
}
