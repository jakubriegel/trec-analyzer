package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.api.IndexAlgorithm
import eu.jrie.put.trec.api.IndexAlgorithm.BM25
import eu.jrie.put.trec.api.IndexAlgorithm.BM25_PLUS_DFR
import eu.jrie.put.trec.api.IndexAlgorithm.DFR
import eu.jrie.put.trec.api.IndexAlgorithm.DFR_BM25
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class Repository {
    suspend fun find(query: String, algorithm: IndexAlgorithm): Flow<ArticleMatch> {
        logger.info("Finding \"$query\" by $algorithm.")
        return when (algorithm) {
            BM25 -> findByBM25(query)
            DFR -> findByDFR(query)
            DFR_BM25 -> findByDFRBM25(query)
            BM25_PLUS_DFR -> findByBM25AndDFR(query)
        }
    }

    protected abstract suspend fun findByDFR(query: String): Flow<ArticleMatch>

    protected abstract suspend fun findByBM25(query: String): Flow<ArticleMatch>

    protected abstract suspend fun findByDFRBM25(query: String): Flow<ArticleMatch>

    private suspend fun findByBM25AndDFR(query: String) = flow {
            findByBM25(query).collect { emit(it) }
            findByDFR(query).collect { emit(it) }
        } .toList()
        .distinctBy { it.article.id }
        .sortedByDescending { it.score }
        .asFlow()
        .withIndex()
        .map { (i, match) -> ArticleMatch(i+1, match.score, match.article) }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(Repository::class.java)
    }
}
