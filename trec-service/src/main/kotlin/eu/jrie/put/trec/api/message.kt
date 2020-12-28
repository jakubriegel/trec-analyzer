package eu.jrie.put.trec.api

import eu.jrie.put.trec.domain.eval.EvalResult
import eu.jrie.put.trec.domain.index.ArticleMatch
import eu.jrie.put.trec.domain.query.Query

data class CustomQueryRequest (
    val query: String,
    val options: QueryOptions
)

data class CustomQueryResponse (
    val query: CustomQueryRequest,
    val documents: List<ArticleMatch>
)

data class QueryRequest (
    val queryId: Int,
    val options: QueryOptions
)

data class QueryResponse (
    val query: Query,
    val options: QueryOptions,
    val documents: List<ArticleMatch>
)

data class EvaluationRequest (
    val name: String,
    val queriesIds: List<Int>,
    val options: QueryOptions
)

data class EvaluationResponse (
    val results: List<EvalResult>,
    val log: String,
    val latex: String
)

data class QueryOptions (
    val algorithm: IndexAlgorithm,
    val engine: IndexEngine
)

data class IndexType (
    val engine: IndexEngine,
    val algorithm: IndexAlgorithm
)

enum class IndexEngine {
    ELASTICSEARCH, TERRIER
}

enum class IndexAlgorithm {
    BM25,
    DFR,
    BM25_PLUS_DFR,
}
