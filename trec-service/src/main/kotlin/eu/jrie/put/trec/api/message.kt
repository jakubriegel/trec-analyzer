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

data class TopicMessage (
    val id: Int,
    val set: String
)

data class QueryRequest (
    val topic: TopicMessage,
    val options: QueryOptions
)

data class QueryResponse (
    val topic: Topic,
    val options: QueryOptions,
    val documents: List<ArticleMatch>
)

data class EvaluateTopicsRequest (
    val name: String,
    val topics: List<TopicMessage>,
    val qrelsSet: String,
    val options: QueryOptions
)

data class EvaluateAllTopicsRequest (
    val name: String,
    val topicSet: String,
    val qrelsSet: String,
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

enum class IndexEngine {
    ELASTICSEARCH, TERRIER
}

enum class IndexAlgorithm {
    BM25, DFR, DFR_BM25, BM25_PLUS_DFR,
}
