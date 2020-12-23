package eu.jrie.put.trec.domain.index.es

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.infra.xmlMapper
import eu.jrie.put.trec.domain.model.Articles
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File

const val articlesPath = "C:\\Users\\jakub\\dev\\zti_trec\\pubmed19n0001.xml"

fun main() {
    createEsIndex()
    File(articlesPath).readText()
        .let { xmlMapper.readValue<Articles>(it) }
        .data.chunked(10000)
        .forEach {
            insertArticles(it)
            logger.info("inserted")
        }
    stopElastic()
}

private val logger: Logger = LoggerFactory.getLogger("esInit")
