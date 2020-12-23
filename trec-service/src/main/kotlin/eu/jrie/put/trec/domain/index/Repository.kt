package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.api.IndexAlgorithm
import eu.jrie.put.trec.api.IndexAlgorithm.*
import eu.jrie.put.trec.domain.model.Article

abstract class Repository {
    suspend fun find(query: String, algorithm: IndexAlgorithm): List<ArticleMatch> = when (algorithm) {
        BM25 -> findByBM25(query)
        DFR -> findByDFR(query)
        BM25_PLUS_DFR -> findByBM25(query) + findByDFR(query)
    }

    protected abstract suspend fun findByDFR(query: String): List<ArticleMatch>

    protected abstract suspend fun findByBM25(query: String): List<ArticleMatch>
}
