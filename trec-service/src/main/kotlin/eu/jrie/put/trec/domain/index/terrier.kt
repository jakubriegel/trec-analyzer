package eu.jrie.put.trec.domain.index

import com.fasterxml.jackson.module.kotlin.readValue
import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.infra.Articles
import eu.jrie.put.trec.infra.config
import eu.jrie.put.trec.infra.jsonMapper
import eu.jrie.put.trec.infra.xmlMapper
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.terrier.indexing.Document
import org.terrier.indexing.FlatJSONDocument
import org.terrier.querying.ManagerFactory
import org.terrier.querying.SearchRequest.CONTROL_WMODEL
import org.terrier.structures.IndexOnDisk
import org.terrier.structures.IndexUtil
import org.terrier.structures.indexing.classical.BasicIndexer
import org.terrier.structures.merging.StructureMerger
import org.terrier.utility.ApplicationSetup
import java.io.File
import java.util.UUID.randomUUID
import java.util.concurrent.ForkJoinPool
import java.util.stream.Stream
import kotlin.coroutines.CoroutineContext


private data class FlatArticle(
    val id: String,
    val text: String,
    val process: String = "text"
)

private fun Article.flatten() = FlatArticle(
    id.toString(), "$title $abstract ${keywords.joinToString(" ")} ${meshHeadings.map { "${it.name} ${it.description}" }.joinToString(" ")}"
)

class FlatArticleCollection(
    articles: Sequence<Article>
) : org.terrier.indexing.Collection {

    private val flatArticles: Iterator<Document> = articles
        .map { it.flatten() }
        .map { jsonMapper.writeValueAsString(it) }
        .map { FlatJSONDocument(it) }
        .iterator()

    override fun close() = Unit
    override fun nextDocument() = flatArticles.hasNext()
    override fun getDocument() = flatArticles.next()
    override fun endOfCollection() = !flatArticles.hasNext()
    override fun reset() = throw IllegalStateException("reset")
}

private const val INDEX_PATH = "/terrier_data"
private const val INDEX_PREFIX = "trec"

fun initTerrier() {
    logger.info("Initializing terrier index")

    ApplicationSetup.setProperty("indexer.meta.forward.keys", "id")
    ApplicationSetup.setProperty("indexer.meta.forward.keylens", "20")

    indexThreaded()
}

fun readArticlesCool(articlesPath: String = "/corpus"): Stream<Sequence<Article>> {
    val n = config.getLong("init.corpusFiles")
    return File(articlesPath).listFiles()!!
        .toList()
        .stream()
        .let { if (n > 0) it.limit(n) else it }
        .map { logger.info("readText"); it.readText() }
        .map { logger.info("xmlMapper"); xmlMapper.readValue<Articles>(it) }
        .map { it.data }
}

private fun indexThreaded() {
    val nThreads = config.getInt("terrier.init.workers")

    val pool = ForkJoinPool(nThreads)
    val action: () -> String = {
        logger.info("Terrier index creation started")
        readArticlesCool()
            .parallel()
            .map { articles ->
                logger.info("map")
                val prefix = nextPrefix
                val indexer = BasicIndexer(INDEX_PATH, prefix)
                val collection = FlatArticleCollection(articles)
                indexer.index(arrayOf(collection))
                prefix
            }
            .reduce { first, second ->
                logger.info("reduce $first $second")
                val src1 = IndexOnDisk.createIndex(INDEX_PATH, first)
                val src2 = IndexOnDisk.createIndex(INDEX_PATH, second)

                val prefix = nextPrefix
                val newIndex = IndexOnDisk.createNewIndex(INDEX_PATH, prefix)
                StructureMerger(src1, src2, newIndex).mergeStructures()

                src1.close()
                src2.close()
                newIndex.close()
                IndexUtil.deleteIndex(INDEX_PATH, first)
                IndexUtil.deleteIndex(INDEX_PATH, second)
                prefix
            }
            .get()
    }
    masterPrefix = pool.submit(action).get()

    logger.info("Created terrier index: $masterPrefix")
}

private val nextPrefix: String
    get() = "${INDEX_PREFIX}_${randomUUID()}"

private lateinit var masterPrefix: String

class TerrierRepository(
    private val context: CoroutineContext
) : Repository() {

    private val elasticsearchRepository = ElasticsearchRepository(context)
    private val index = IndexOnDisk.createIndex(INDEX_PATH, masterPrefix)
    private val queryingManager = runBlocking(context) { ManagerFactory.from(index.indexRef) }

    init {
        ApplicationSetup.setProperty(
            "querying.processes", "terrierql:TerrierQLParser,"
                    + "parsecontrols:TerrierQLToControls,"
                    + "parseql:TerrierQLToMatchingQueryTerms,"
                    + "matchopql:MatchingOpQLParser,"
                    + "applypipeline:ApplyTermPipeline,"
                    + "localmatching:LocalManager\$ApplyLocalMatching,"
                    + "filters:LocalManager\$PostFilterProcess"
        )

        ApplicationSetup.setProperty("querying.postfilters", "decorate:org.terrier.querying.SimpleDecorate")
    }

    override suspend fun findByDFR(query: String) = search(query, "BB2")

    override suspend fun findByBM25(query: String) = search(query, "BM25")

    private suspend fun search(query: String, model: String) = withContext(context) {
        logger.info("Querying terrier for \"$query\" by $model")
        val request = queryingManager.newSearchRequestFromQuery(query)
            .apply {
                setControl(CONTROL_WMODEL, model)
                setControl("end", "20")
                setControl("terrierql", "on")
                setControl("parsecontrols", "on")
                setControl("parseql", "on")
                setControl("applypipeline", "on")
                setControl("localmatching", "on")
                setControl("filters", "on")
                setControl("decorate", "on")
            }

        queryingManager.runSearchRequest(request)
        request.results
            .asFlow()
            .withIndex()
            .map { (i, doc) ->
                val article = elasticsearchRepository.get(doc.getMetadata("id"))
                ArticleMatch(i + 1, doc.score.toFloat(), article)
            }
    }
}

private val logger: Logger = LoggerFactory.getLogger("terrier")
