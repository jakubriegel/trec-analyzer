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

private const val DEFAULT_ARTICLES_PATH = "C:\\Users\\jakub\\dev\\zti_trec\\pubmed19n0001.xml"

fun readArticles(articlesPath: String = DEFAULT_ARTICLES_PATH): Sequence<Article> {
    return File(articlesPath).readText()
        .let { xmlMapper.readValue<Articles>(it) }
        .data
}
