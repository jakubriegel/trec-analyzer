package eu.jrie.put.trec.domain.index.terrier

import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.index.es.jsonMapper
import eu.jrie.put.trec.domain.index.es.stopElastic
import eu.jrie.put.trec.domain.readArticles
import eu.jrie.put.trec.infra.xmlMapper
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper.Indexer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.terrier.applications.BatchIndexing
import org.terrier.indexing.Document
import org.terrier.indexing.FlatJSONDocument
import org.terrier.indexing.SimpleFileCollection
import org.terrier.querying.ManagerFactory
import org.terrier.querying.SearchRequest.CONTROL_WMODEL
import org.terrier.structures.Index
import org.terrier.structures.IndexOnDisk
import org.terrier.structures.indexing.classical.BasicIndexer
import org.terrier.utility.ApplicationSetup
import java.io.File
import javax.naming.directory.SearchResult


data class FlatArticle(
    val id: String,
    val text: String,
    val process: String = "text"
)

fun Article.flatten() = FlatArticle(id.toString(), "$title $abstract")

class FlatArticleCollection (
    articles: Sequence<Article>
) : org.terrier.indexing.Collection {

    private val flatArticles: Iterator<Document> = articles //.take(500)
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

const val INDEX_PATH = "C:\\Users\\jakub\\Dev\\zti_trec\\trec-service\\terrier"

private fun createIndex() {
    ApplicationSetup.setProperty("indexer.meta.forward.keys", "id");
    ApplicationSetup.setProperty("indexer.meta.forward.keylens", "20");

    val indexer = BasicIndexer(INDEX_PATH, "data")
    val coll = FlatArticleCollection(readArticles())
    indexer.index(arrayOf(coll))
}

fun query() {
    val index = IndexOnDisk.createIndex(INDEX_PATH, "data");

    // Set up the querying process
    ApplicationSetup.setProperty("querying.processes", "terrierql:TerrierQLParser,"
            + "parsecontrols:TerrierQLToControls,"
            + "parseql:TerrierQLToMatchingQueryTerms,"
            + "matchopql:MatchingOpQLParser,"
            + "applypipeline:ApplyTermPipeline,"
            + "localmatching:LocalManager\$ApplyLocalMatching,"
            + "filters:LocalManager\$PostFilterProcess");

    // Enable the decorate enhancement
    ApplicationSetup.setProperty("querying.postfilters", "decorate:org.terrier.querying.SimpleDecorate");

    // Create a new manager run queries
    val queryingManager = ManagerFactory.from(index.indexRef);

    // Create a search request
    val srq = queryingManager.newSearchRequestFromQuery("virus")

    // Specify the model to use when searching
    srq.setControl(CONTROL_WMODEL, "BM25");

    // Enable querying processes
    srq.setControl("terrierql", "on");
    srq.setControl("parsecontrols", "on");
    srq.setControl("parseql", "on");
    srq.setControl("applypipeline", "on");
    srq.setControl("localmatching", "on");
    srq.setControl("filters", "on");

    // Enable post filters
    srq.setControl("decorate", "on");

    // Run the search
    queryingManager.runSearchRequest(srq);

    // Get the result set
    val results = srq.results

    // Print the results
    logger.info("The top ${results.size} of documents were returned");
    logger.info("Document Ranking");
    results.take(15)
        .forEachIndexed { i, doc ->
            logger.info("\tRank ${i + 1}: ${doc.getMetadata("id")} ${doc.score}");
        }
}

fun main() {
    stopElastic()
    createIndex()
    query()
}

private val logger: Logger = LoggerFactory.getLogger("terrierInit")
