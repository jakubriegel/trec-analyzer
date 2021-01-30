package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.MeshHeading
import eu.jrie.put.trec.domain.readArticlesStream
import eu.jrie.put.trec.infra.config
import eu.jrie.put.trec.infra.jsonMapper
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
import org.terrier.structures.IndexOnDisk.createIndex
import org.terrier.structures.IndexOnDisk.createNewIndex
import org.terrier.structures.IndexUtil.renameIndex
import org.terrier.structures.indexing.classical.BasicIndexer
import org.terrier.structures.merging.BlockStructureMerger
import org.terrier.utility.ApplicationSetup
import java.util.UUID.randomUUID
import java.util.concurrent.ForkJoinPool
import kotlin.coroutines.CoroutineContext


data class TerrierArticle(
    val id: Int,
    val title: String,
    val abstract: String,
    val keywords: List<String>,
    val meshHeadings: List<MeshHeading>,
    val process: String = "id,title,abstract,keywords,meshHeadings"
)


class FlatArticleCollection(
    articles: Sequence<Article>
) : org.terrier.indexing.Collection {

    private val flatArticles: Iterator<Document> = articles
        .map { TerrierArticle(it.id, it.title, it.abstract, it.keywords, it.meshHeadings) }
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
private val nextPrefix: String
    get() = "${INDEX_PREFIX}_${randomUUID()}"

fun initTerrier() {
    logger.info("Initializing terrier index")

    ApplicationSetup.setProperty("indexer.meta.forward.keys", "id")
    ApplicationSetup.setProperty("indexer.meta.forward.keylens", "20")

    indexThreaded()
}

private fun mergeIndices(first: IndexOnDisk, second: IndexOnDisk): IndexOnDisk {
    val newIndex = createNewIndex(INDEX_PATH, nextPrefix)
    logger.info("Merging ${first.prefix} and ${second.prefix} into ${newIndex.prefix}")
    logger.info("First\n${first.collectionStatistics}")
    logger.info("First\n${first.properties}")
    logger.info("Second\n${second.collectionStatistics}")
    logger.info("Second\n${second.properties}")
    BlockStructureMerger(first, second, newIndex).mergeStructures()
    first.close()
    second.close()
//    deleteIndex (INDEX_PATH, first.prefix)
//    deleteIndex(INDEX_PATH, second.prefix)
    return newIndex
}

private fun indexThreaded() {
    val nThreads = config.getInt("terrier.init.workers")
    val action: () -> String = {
        logger.info("Terrier index creation started")
        readArticlesStream()
            .parallel()
            .map { articles ->
                logger.info("indexing chunk")
                val prefix = nextPrefix
                val indexer = BasicIndexer(INDEX_PATH, prefix)
                indexer.externalParalllism = nThreads
                val collection = FlatArticleCollection(articles)
                indexer.index(arrayOf(collection))
                createIndex(INDEX_PATH, prefix)
            }
            .reduce { first, second ->
                logger.info("reduce indices")
                mergeIndices(first, second)
            }
            .map { renameIndex(INDEX_PATH, it.prefix, INDEX_PATH, INDEX_PREFIX) }
            .map { INDEX_PREFIX }
            .get()
    }
    val masterPrefix = ForkJoinPool(nThreads).submit(action).get()
    logger.info("Created terrier index: $masterPrefix")
}

class TerrierRepository(
    private val context: CoroutineContext
) : Repository() {

    private val elasticsearchRepository = ElasticsearchRepository(context)
    private val index = createIndex(INDEX_PATH, INDEX_PREFIX)
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

    override suspend fun findByDFRBM25(query: String) = search(query, "DFR_BM25")

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
