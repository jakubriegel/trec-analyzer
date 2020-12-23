package eu.jrie.put.trec.domain.index.es

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import eu.jrie.put.trec.domain.index.ArticleMatch
import eu.jrie.put.trec.domain.index.Repository
import eu.jrie.put.trec.domain.model.Article
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.http.HttpHost
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import kotlin.coroutines.CoroutineContext

class ElasticsearchRepository (
    private val context: CoroutineContext
) : Repository() {

    private val jsonMapper = JsonMapper().registerKotlinModule()

    private val client = runBlocking(context) {
        RestHighLevelClient(
            RestClient.builder(host)
        )
    }

    override suspend fun findByDFR(query: String) = submit(query, "trec_dfr")

    override suspend fun findByBM25(query: String) = submit(query, "trec_bm25")

    private suspend fun submit(query: String, index: String): List<ArticleMatch> {
        return withContext(context) {
            SearchSourceBuilder()
                .query(QueryBuilders.multiMatchQuery(query, "title", "content"))
                .let { SubmitAsyncSearchRequest(it, index) }
                .let {
                    @Suppress("BlockingMethodInNonBlockingContext")
                    client.asyncSearch()
                        .submit(it, RequestOptions.DEFAULT)
                }
                .searchResponse.hits.hits
                .asSequence()
                .map { hit ->
                    val article: Article = jsonMapper.readValue(hit.sourceAsString)
                    ArticleMatch(hit.score, article)
                }
                .sortedByDescending { it.score }
                .toList()
        }
    }

    companion object {
        private const val ES_HOST = "elasticsearch"
        private const val ES_PORT = 9200
        private val host = HttpHost(ES_HOST, ES_PORT, "http")
    }
}
