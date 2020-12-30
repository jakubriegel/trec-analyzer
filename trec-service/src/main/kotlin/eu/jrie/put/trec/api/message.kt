package eu.jrie.put.trec.api

import eu.jrie.put.trec.domain.eval.EvalResult
import eu.jrie.put.trec.domain.index.ArticleMatch
import eu.jrie.put.trec.domain.query.Topic

data class CustomQueryRequest (
    val query: String,
    val options: QueryOptions
)

data class CustomQueryResponse (
    val query: CustomQueryRequest,
    val documents: List<ArticleMatch>
)

data class QueryRequest (
    val topicId: Int,
    val options: QueryOptions
)

data class QueryResponse (
    val topic: Topic,
    val options: QueryOptions,
    val documents: List<ArticleMatch>
)

data class EvaluateQrelsResponse (
    val documentId: Int,
    val topicId: Int,
    val isRelevant: Boolean?
)

data class EvaluateTopicsRequest (
    val name: String,
    val topicsIds: List<Int>,
    val options: QueryOptions
)

data class EvaluateAllTopicsRequest (
    val name: String,
    val options: QueryOptions
)

data class EvaluateTopicsResponse (
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
