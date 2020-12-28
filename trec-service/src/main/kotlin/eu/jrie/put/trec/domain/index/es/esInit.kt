package eu.jrie.put.trec.domain.index.es

import eu.jrie.put.trec.domain.readArticles
import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun main() {
    createEsIndex()
    readArticles()
        .chunked(10000)
        .forEach {
            insertArticles(it)
            logger.info("inserted")
        }
    stopElastic()
}

private val logger: Logger = LoggerFactory.getLogger("esInit")
