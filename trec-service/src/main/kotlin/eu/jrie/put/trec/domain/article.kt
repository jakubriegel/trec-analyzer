package eu.jrie.put.trec.domain

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.infra.Articles
import eu.jrie.put.trec.infra.config
import eu.jrie.put.trec.infra.xmlMapper
import java.io.File
import java.util.stream.Stream

data class Article(
    val id: Int,
    val title: String,
    val abstract: String,
    val keywords: List<String>,
    val meshHeadings: List<MeshHeading>
)

data class MeshHeading(
    val description: String,
    val name: String
)

private const val DEFAULT_ARTICLES_PATH = "/corpus"

private val filesLimit = config.getInt("init.corpusFiles")

private val articlesFiles: List<File>
    get() = File(DEFAULT_ARTICLES_PATH).listFiles()!!
        .let { if (filesLimit > 0) it.take(filesLimit) else it.toList() }

fun readArticles(): Sequence<Sequence<Article>> {
    return articlesFiles.asSequence()
        .map { it.readText() }
        .map { xmlMapper.readValue<Articles>(it) }
        .map { it.data }
}

fun readArticlesStream(): Stream<Sequence<Article>> {
    return articlesFiles.stream()
        .map { it.readText() }
        .map { xmlMapper.readValue<Articles>(it) }
        .map { it.data }
}
