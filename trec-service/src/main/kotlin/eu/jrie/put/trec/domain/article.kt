package eu.jrie.put.trec.domain

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.infra.Articles
import eu.jrie.put.trec.infra.config
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

fun readArticles(articlesPath: String = DEFAULT_ARTICLES_PATH): Pair<Int, Sequence<Sequence<Article>>> {
    val n = config.getInt("init.corpusFiles")
    val files =  File(articlesPath).listFiles()!!
    return files.size to files.asSequence()
        .let { if (n > 0) it.take(n) else it }
        .map { it.readText() }
        .map { xmlMapper.readValue<Articles>(it) }
        .map { it.data }
}
