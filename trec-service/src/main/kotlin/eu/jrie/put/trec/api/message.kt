package eu.jrie.put.trec.api

import eu.jrie.put.trec.domain.eval.EvaluationData
import eu.jrie.put.trec.api.IndexAlgorithm.*
import eu.jrie.put.trec.domain.index.ArticleMatch
import eu.jrie.put.trec.domain.model.Article
import org.elasticsearch.search.SearchHit

data class QueryRequest (
    val query: String,
    val options: QueryOptions
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
    ELASTICSEARCH, PYTERRIER
}

enum class IndexAlgorithm {
    BM25,
    DFR,
    BM25_PLUS_DFR,
}

data class QueryResponse (
    val query: QueryRequest,
    val documents: List<ArticleMatch>
)

data class EvaluationRequest (
    val name: String,
    val data: List<EvaluationData>
)
