package eu.jrie.put.trec.domain.index.es

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.index.ArticleMatch
import eu.jrie.put.trec.domain.index.Repository
import eu.jrie.put.trec.infra.jsonMapper
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.http.HttpHost
import org.elasticsearch.action.get.GetRequest
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

    private val client = runBlocking(context) {
        RestHighLevelClient(
            RestClient.builder(ELASTICSEARCH_HOST)
        )
    }

    override suspend fun findByDFR(query: String) = submit(query, "trec_dfr")

    override suspend fun findByBM25(query: String) = submit(query, "trec_bm25")

    private suspend fun submit(query: String, index: String) = withContext(context) {
        SearchSourceBuilder()
            .query(QueryBuilders.multiMatchQuery(query, "title", "content"))
            .let { SubmitAsyncSearchRequest(it, index) }
            .let {
                @Suppress("BlockingMethodInNonBlockingContext")
                client.asyncSearch()
                    .submit(it, RequestOptions.DEFAULT)
            }
            .searchResponse.hits.hits
            .sortedByDescending { it.score }
            .asFlow()
            .withIndex()
            .map { (i, hit) ->
                val article: Article = jsonMapper.readValue(hit.sourceAsString)
                ArticleMatch(i+1, hit.score, article)
            }
    }

    suspend fun get(id: String): Article = withContext(context) {
        val request = GetRequest("trec_bm25", id)
        @Suppress("BlockingMethodInNonBlockingContext")
        client.get(request, RequestOptions.DEFAULT)
            .sourceAsString
            .let { jsonMapper.readValue(it) as Article }
    }
}
