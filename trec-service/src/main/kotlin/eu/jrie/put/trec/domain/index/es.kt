package eu.jrie.put.trec.domain.index

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.readArticles
import eu.jrie.put.trec.infra.config
import eu.jrie.put.trec.infra.jsonMapper
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.*
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
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
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

private const val ES_HOST = "elasticsearch"
private const val ES_PORT = 9200
val ELASTICSEARCH_HOST = HttpHost(ES_HOST, ES_PORT, "http")

private val client = RestHighLevelClient(
    RestClient.builder(ELASTICSEARCH_HOST)
)

@ExperimentalCoroutinesApi
fun initEs() = runBlocking {
    logger.info("Initializing ES index")
    createEsIndexes()

    val chunkSize = config.getInt("es.init.chunkSize")
    val articles = produce {
        readArticles()
            .chunked(chunkSize)
            .forEachIndexed{ i, chunk ->
                if ((i % 100) == 0) logger.info("processed ${i * chunkSize} articles")
                send(chunk)
            }
    }
    val workers = List(config.getInt("es.init.workers")) {
        launch {
            articles.consumeAsFlow()
                .collect {
                    insertArticles(it, "trec_bm25")
                    insertArticles(it, "trec_dfr")
                }
        }
    }
    workers.forEach { it.join() }
    client.close()
}

private tailrec suspend fun pingEs() {
    val result = runCatching { client.ping(RequestOptions.DEFAULT) }
    if (result.isSuccess) logger.info("Test es ping ok=${result.getOrNull()}")
    else {
        logger.info("Test es ping ok=NOT_AVAILABLE")
        delay(3000)
        pingEs()
    }
}

private suspend fun createEsIndexes() {
    pingEs()
    createIndex("trec_bm25", """
        {
            "type": "BM25",
            "b": 0.75, 
            "k1": 1.2
        }
    """)
    createIndex("trec_dfr", """
        {
            "type": "DFR",
            "basic_model": "g", 
            "after_effect": "b",
            "normalization": "h2"
        }
    """)
}

private fun createIndex(name: String, type: String) {
    runCatching {
        logger.info("Deleting $name index")
        val requestDel = DeleteIndexRequest(name)
        val delIndexResponse = client.indices().delete(requestDel, RequestOptions.DEFAULT)
        logger.info("Deleting $name index ok=${delIndexResponse.isAcknowledged}")
    }

    logger.info("Creating $name index")
    val request = CreateIndexRequest(name)
    request.settings(
        """{
            "number_of_shards": 1,
            "similarity": {
                 "default": $type
            }
        }""",
        XContentType.JSON
    )

    val createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT)
    logger.info("Creating $name index ok=${createIndexResponse.isAcknowledged}")
}

private fun insertArticle(article: Article) {
    val request = IndexRequest("trec_bm25")
        .id(article.id.toString())
        .source(jsonMapper.writeValueAsString(article), XContentType.JSON)
        .opType(DocWriteRequest.OpType.CREATE)
    val indexResponse = client.index(request, RequestOptions.DEFAULT)
    logger.info(indexResponse.toString())
}

@ExperimentalCoroutinesApi
private suspend fun insertArticles(articles: List<Article>, index: String) {
    val request = BulkRequest()
    articles.asSequence()
        .map {
            IndexRequest(index)
                .id(it.id.toString())
                .source(jsonMapper.writeValueAsString(it), XContentType.JSON)
                .opType(DocWriteRequest.OpType.CREATE)
        }
        .forEach { request.add(it) }

    suspendCancellableCoroutine<Unit> { continuation ->
        val callback = object : ActionListener<BulkResponse> {
            override fun onResponse(response: BulkResponse?) {
                logger.info("bulk insert into $index ok")
                continuation.resume(Unit)
            }

            override fun onFailure(e: Exception) {
                logger.error("bulk insert into $index failed", e)
                continuation.resumeWithException(e)
            }
        }
        logger.info("bulk insert into $index start")
        client.bulkAsync(request, RequestOptions.DEFAULT, callback)
    }


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
            .size(20)
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
