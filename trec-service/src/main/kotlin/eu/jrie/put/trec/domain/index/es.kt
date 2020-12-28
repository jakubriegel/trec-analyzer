package eu.jrie.put.trec.domain.index

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.readArticles
import eu.jrie.put.trec.infra.jsonMapper
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.http.HttpHost
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext


private const val ES_HOST = "elasticsearch"
private const val ES_PORT = 9200
val ELASTICSEARCH_HOST = HttpHost(ES_HOST, ES_PORT, "http")

private val client = RestHighLevelClient(
    RestClient.builder(ELASTICSEARCH_HOST)
)

fun initEs() {
    logger.info("Initializing ES index")
    createEsIndex()
    readArticles()
        .chunked(10000)
        .forEach { insertArticles(it) }
    client.close()
}

private fun createEsIndex() {
    val ping = client.ping(RequestOptions.DEFAULT)
    logger.info("Test es ping ok=$ping")

    runCatching {
        logger.info("Deleting es index")
        val requestDel = DeleteIndexRequest("trec_bm25")
        val delIndexResponse = client.indices().delete(requestDel, RequestOptions.DEFAULT)
        logger.info("Deleting index ok=${delIndexResponse.isAcknowledged}")
    }

    logger.info("Creating es index")
    val request = CreateIndexRequest("trec_bm25")
    request.settings(
        """{
            "number_of_shards": 1,
            "similarity": {
                 "trec_bm25": {
                    "type": "BM25",
                    "b": 0, 
                    "k1": 0.9
                 }
            }
        }""",
        XContentType.JSON
    )

    request.mapping(
        """{
                  "properties": {
                    "message": {
                      "type": "text"
                    }
                  }
                }""",
        XContentType.JSON)

    val createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT)
    logger.info("Creating index ok=${createIndexResponse.isAcknowledged}")
}

private fun insertArticle(article: Article) {
    val request = IndexRequest("trec_bm25")
        .id(article.id.toString())
        .source(jsonMapper.writeValueAsString(article), XContentType.JSON)
        .opType(DocWriteRequest.OpType.CREATE)
    val indexResponse = client.index(request, RequestOptions.DEFAULT)
    logger.info(indexResponse.toString())
}

private fun insertArticles(articles: List<Article>) {
    val inserts = articles.map {
        IndexRequest("trec_bm25")
            .id(it.id.toString())
            .source(jsonMapper.writeValueAsString(it), XContentType.JSON)
            .opType(DocWriteRequest.OpType.CREATE)
    }
    val request = BulkRequest().add(inserts)
    client.bulk(request, RequestOptions.DEFAULT)
    logger.info("bulk insert done")
}

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

private val logger: Logger = LoggerFactory.getLogger("es")
