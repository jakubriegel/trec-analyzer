package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.api.IndexAlgorithm
import eu.jrie.put.trec.api.IndexAlgorithm.*
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class Repository {
    suspend fun find(query: String, algorithm: IndexAlgorithm): List<ArticleMatch> {
        logger.info("Finding \"$query\" by $algorithm.")
        return when (algorithm) {
            BM25 -> findByBM25(query)
            DFR -> findByDFR(query)
            BM25_PLUS_DFR -> findByBM25AndDFR(query)
        } .take(25).toList()
    }

    protected abstract suspend fun findByDFR(query: String): Flow<ArticleMatch>

    protected abstract suspend fun findByBM25(query: String): Flow<ArticleMatch>

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
