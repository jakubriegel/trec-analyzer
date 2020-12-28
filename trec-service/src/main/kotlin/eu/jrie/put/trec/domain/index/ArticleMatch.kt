package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.domain.Article

data class ArticleMatch (
    val rank: Int,
    val score: Float,
    val article: Article
)
