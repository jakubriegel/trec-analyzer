package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.domain.model.Article

data class ArticleMatch (
    val score: Float,
    val article: Article
)
