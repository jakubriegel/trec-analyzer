package eu.jrie.put.trec.domain

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.infra.Articles
import eu.jrie.put.trec.infra.xmlMapper
import java.io.File

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

fun readArticles(articlesPath: String = DEFAULT_ARTICLES_PATH): Sequence<Article> {
    return File(articlesPath).listFiles()!!
        .asSequence()
        .map { it.readText() }
        .map { xmlMapper.readValue<Articles>(it) }
        .flatMap { it.data }
}
