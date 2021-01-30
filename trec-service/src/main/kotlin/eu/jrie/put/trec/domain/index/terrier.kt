package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.MeshHeading
import eu.jrie.put.trec.domain.readArticlesStream
import eu.jrie.put.trec.infra.config
import eu.jrie.put.trec.infra.jsonMapper
import gnu.trove.TIntIntHashMap
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
import org.terrier.structures.AbstractPostingOutputStream
import org.terrier.structures.BasicDocumentIndexEntry
import org.terrier.structures.BitIndexPointer
import org.terrier.structures.DocumentIndex
import org.terrier.structures.DocumentIndexEntry
import org.terrier.structures.FSOMapFileLexiconOutputStream
import org.terrier.structures.FieldDocumentIndexEntry
import org.terrier.structures.FieldLexiconEntry
import org.terrier.structures.IndexOnDisk
import org.terrier.structures.IndexOnDisk.createIndex
import org.terrier.structures.IndexOnDisk.createNewIndex
import org.terrier.structures.IndexUtil
import org.terrier.structures.IndexUtil.deleteIndex
import org.terrier.structures.IndexUtil.renameIndex
import org.terrier.structures.LexiconEntry
import org.terrier.structures.LexiconOutputStream
import org.terrier.structures.Pointer
import org.terrier.structures.PostingIndex
import org.terrier.structures.PostingIndexInputStream
import org.terrier.structures.SimpleBitIndexPointer
import org.terrier.structures.SimpleDocumentIndexEntry
import org.terrier.structures.bit.DirectInvertedOutputStream
import org.terrier.structures.bit.FieldDirectInvertedOutputStream
import org.terrier.structures.indexing.CompressingMetaIndexBuilder
import org.terrier.structures.indexing.CompressionFactory
import org.terrier.structures.indexing.CompressionFactory.CompressionConfiguration
import org.terrier.structures.indexing.DocumentIndexBuilder
import org.terrier.structures.indexing.LexiconBuilder
import org.terrier.structures.indexing.MetaIndexBuilder
import org.terrier.structures.indexing.classical.BasicIndexer
import org.terrier.structures.merging.LexiconMerger
import org.terrier.structures.postings.IterablePosting
import org.terrier.structures.postings.Posting
import org.terrier.structures.postings.PostingIdComparator
import org.terrier.structures.postings.bit.BasicIterablePosting
import org.terrier.structures.postings.bit.FieldIterablePosting
import org.terrier.structures.seralization.FixedSizeWriteableFactory
import org.terrier.utility.ApplicationSetup
import org.terrier.utility.ArrayUtils
import java.io.IOException
import java.util.*
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
//    logger.info("First\n${first.collectionStatistics}")
//    logger.info("Second\n${second.collectionStatistics}")
    StructureMergerCustom(first, second, newIndex).mergeStructures()
    first.close()
    second.close()
    deleteIndex(INDEX_PATH, first.prefix)
    deleteIndex(INDEX_PATH, second.prefix)
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

open class StructureMergerCustom(
    /** source index 1  */
    protected var srcIndex1: IndexOnDisk,
    /** source index 2  */
    protected var srcIndex2: IndexOnDisk, _destIndex: IndexOnDisk
) {
    class NullDocumentIndex(var numDocs: Int) : DocumentIndex {
        @Throws(IOException::class)
        override fun getDocumentEntry(docid: Int): DocumentIndexEntry {
            throw UnsupportedOperationException()
        }

        @Throws(IOException::class)
        override fun getDocumentLength(docid: Int): Int {
            assert(docid < numDocs)
            return 0
        }

        override fun getNumberOfDocuments(): Int {
            return numDocs
        }
    }

    /**
     * A hashmap for converting the codes of terms appearing only in the
     * vocabulary of the second set of data structures into a new set of
     * term codes for the merged set of data structures.
     */
    protected var termcodeHashmap: TIntIntHashMap? = null
    protected var keepTermCodeMap = false

    /** The number of documents in the merged structures.  */
    protected var numberOfDocuments: Int

    /** The number of pointers in the merged structures.  */
    protected var numberOfPointers: Long

    /** The number of terms in the collection.  */
    protected var numberOfTerms: Int
    protected var compressionDirectConfig: CompressionConfiguration
    protected var compressionInvertedConfig: CompressionConfiguration
    protected var MetaReverse =
        java.lang.Boolean.parseBoolean(ApplicationSetup.getProperty("merger.meta.reverse", "true"))

    /** destination index  */
    protected var destIndex: IndexOnDisk

    /** class to use to write direct file  */
    protected var directFileOutputStreamClass: Class<out DirectInvertedOutputStream?> =
        DirectInvertedOutputStream::class.java
    protected var fieldDirectFileOutputStreamClass: Class<out DirectInvertedOutputStream?> =
        FieldDirectInvertedOutputStream::class.java
    protected val fieldCount: Int
    protected var basicInvertedIndexPostingIteratorClass = BasicIterablePosting::class.java.name
    protected var fieldInvertedIndexPostingIteratorClass = FieldIterablePosting::class.java.name
    protected var basicDirectIndexPostingIteratorClass = BasicIterablePosting::class.java.name
    protected var fieldDirectIndexPostingIteratorClass = FieldIterablePosting::class.java.name

    /**
     * Sets the output index. This index should have no documents
     * @param _outputIndex the index to be merged to
     */
    fun setOutputIndex(_outputIndex: IndexOnDisk) {
        destIndex = _outputIndex
        //invertedFileOutput = _outputName;
    }

    /**
     * Merges the two lexicons into one. After this stage, the offsets in the
     * lexicon are ot correct. They will be updated only after creating the
     * inverted file.
     */
    protected open fun mergeInvertedFiles() {
        try {
            //getting the number of entries in the first document index,
            //in order to assign the correct docids to the documents
            //of the second inverted file.
            val numberOfDocs1 = srcIndex1.collectionStatistics.numberOfDocuments
            val numberOfDocs2 = srcIndex2.collectionStatistics.numberOfDocuments
            numberOfDocuments = numberOfDocs1 + numberOfDocs2
            val srcFieldCount1 = srcIndex1.collectionStatistics.numberOfFields
            val srcFieldCount2 = srcFieldCount1 // srcIndex2.collectionStatistics.numberOfFields
            if (srcFieldCount1 != srcFieldCount2) {
                throw Error("FieldCounts in source indices must match")
            }
            val fieldCount = srcFieldCount1

            //creating a new map between new and old term codes
            if (keepTermCodeMap) termcodeHashmap = TIntIntHashMap()
            logger.debug("Opening src lexicons")
            //setting the input streams
            val lexInStream1 =
                srcIndex1.getIndexStructureInputStream("lexicon") as Iterator<Map.Entry<String, LexiconEntry>>
            val lexInStream2 =
                srcIndex2.getIndexStructureInputStream("lexicon") as Iterator<Map.Entry<String, LexiconEntry>>
            for (property: String? in arrayOf(
                "index.inverted.fields.names",
                "max.term.length",
                "index.lexicon-keyfactory.class",
                "index.lexicon-keyfactory.parameter_values",
                "index.lexicon-keyfactory.parameter_types",
                "index.lexicon-valuefactory.class",
                "index.lexicon-valuefactory.parameter_values",
                "index.lexicon-valuefactory.parameter_types",
                "termpipelines"
            )) {
                destIndex.setIndexProperty(property, srcIndex1.getIndexProperty(property, null))
            }
            val lvf = srcIndex1.getIndexStructure("lexicon-valuefactory") as FixedSizeWriteableFactory<LexiconEntry>
            logger.debug("Opening new target lexicon")
            //setting the output stream
            val lexOutStream: LexiconOutputStream<String> = FSOMapFileLexiconOutputStream(
                destIndex,
                "lexicon",
                lvf.javaClass as Class<FixedSizeWriteableFactory<LexiconEntry?>?>
            )
            var newCodes = if (keepTermCodeMap) srcIndex1.collectionStatistics.numberOfUniqueTerms else 0
            logger.debug("Opening src inv files")
            IndexUtil.forceStructure(
                srcIndex1,
                "document",
                NullDocumentIndex(srcIndex1.collectionStatistics.numberOfDocuments)
            )
            IndexUtil.forceStructure(
                srcIndex2,
                "document",
                NullDocumentIndex(srcIndex2.collectionStatistics.numberOfDocuments)
            )
            val inverted1 = srcIndex1.invertedIndex as PostingIndex<Pointer>
            val inverted2 = srcIndex2.invertedIndex as PostingIndex<Pointer>
            logger.debug("Opening new target inv file")
            var invOS: AbstractPostingOutputStream? = null
            try {
                invOS = compressionInvertedConfig.getPostingOutputStream(
                    destIndex.path + ApplicationSetup.FILE_SEPARATOR +
                            destIndex.prefix + ".inverted" + compressionInvertedConfig.structureFileExtension
                )
            } catch (e: Exception) {
                logger.error("Couldn't create specified AbstractPostingOutputStream", e)
                lexOutStream.close()
                return
            }
            logger.debug("Starting pass through inv & lexicon files")
            var hasMore1 = false
            var hasMore2 = false
            var term1: String
            var term2: String
            var lee1: Map.Entry<String, LexiconEntry>? = null
            var lee2: Map.Entry<String, LexiconEntry>? = null
            hasMore1 = lexInStream1.hasNext()
            if (hasMore1) lee1 = lexInStream1.next()
            hasMore2 = lexInStream2.hasNext()
            if (hasMore2) lee2 = lexInStream2.next()
            while (hasMore1 && hasMore2) {
                term1 = lee1!!.key
                term2 = lee2!!.key
                val lexicographicalCompare = term1.compareTo(term2)
                if (lexicographicalCompare < 0) {
                    //write to inverted file postings for the term that only occurs in 1st index
                    val newPointer = invOS.writePostings(inverted1.getPostings(lee1.value))
                    lee1.value.setPointer(newPointer)
                    numberOfPointers += newPointer.numberOfEntries.toLong()
                    if (!keepTermCodeMap) lee1.value.termId = newCodes++
                    lexOutStream.writeNextEntry(term1, lee1.value)
                    hasMore1 = lexInStream1.hasNext()
                    if (hasMore1) lee1 = lexInStream1.next()
                } else if (lexicographicalCompare > 0) {
                    //write to inverted file postings for the term that only occurs in 2nd index
                    //docids are transformed as we go.
                    val newPointer = invOS.writePostings(inverted2.getPostings(lee2.value), -(numberOfDocs1 + 1))
                    lee2.value.setPointer(newPointer)
                    numberOfPointers += newPointer.numberOfEntries.toLong()
                    val newCode = newCodes++
                    if (keepTermCodeMap) termcodeHashmap!!.put(lee2.value.termId, newCode)
                    lee2.value.termId = newCode
                    lexOutStream.writeNextEntry(term2, lee2.value)
                    hasMore2 = lexInStream2.hasNext()
                    if (hasMore2) lee2 = lexInStream2.next()
                } else {
                    //write to postings for a term that occurs in both indices

                    //1. postings from the first index are unchanged
                    val ip1 = inverted1.getPostings(lee1.value)
                    val newPointer1 = invOS.writePostings(ip1)

                    //2. postings from the 2nd index have their docids transformed
                    val ip2 = inverted2.getPostings(lee2.value)
                    val newPointer2 = invOS.writePostings(ip2, invOS.lastDocidWritten - numberOfDocs1)
                    numberOfPointers += (newPointer1.numberOfEntries + newPointer2.numberOfEntries).toLong()

                    //don't set numberOfEntries, as LexiconEntry.add() will take care of this.
                    lee1.value.setPointer(newPointer1)
                    if (keepTermCodeMap) termcodeHashmap!!.put(
                        lee2.value.termId,
                        lee1.value.termId
                    ) else lee1.value.termId =
                        newCodes++
                    lee1.value.add(lee2.value)
                    lexOutStream.writeNextEntry(term1, lee1.value)
                    hasMore1 = lexInStream1.hasNext()
                    if (hasMore1) lee1 = lexInStream1.next()
                    hasMore2 = lexInStream2.hasNext()
                    if (hasMore2) lee2 = lexInStream2.next()
                }
            }
            if (hasMore1) {
                logger.debug("Now processing trailing terms from lex1")
                lee2 = null
                while (hasMore1) {
                    //write to inverted file as well.
                    val newPointer = invOS.writePostings(
                        inverted1.getPostings(lee1!!.value)
                    )
                    lee1.value.setPointer(newPointer)
                    if (!keepTermCodeMap) lee1.value.termId = newCodes++
                    numberOfPointers += newPointer.numberOfEntries.toLong()
                    lexOutStream.writeNextEntry(lee1.key, lee1.value)
                    hasMore1 = lexInStream1.hasNext()
                    if (hasMore1) lee1 = lexInStream1.next()
                }
            } else if (hasMore2) {
                lee1 = null
                logger.debug("Now processing trailing terms from lex2")
                while (hasMore2) {
                    //write to inverted file as well.
                    val newPointer = invOS.writePostings(
                        inverted2.getPostings(lee2!!.value), -(numberOfDocs1 + 1)
                    )
                    lee2.value.setPointer(newPointer)
                    numberOfPointers += newPointer.numberOfEntries.toLong()
                    val newCode = newCodes++
                    if (keepTermCodeMap) termcodeHashmap!!.put(lee2.value.termId, newCode)
                    lee2.value.termId = newCode
                    lexOutStream.writeNextEntry(lee2.key, lee2.value)
                    hasMore2 = lexInStream2.hasNext()
                    if (hasMore2) lee2 = lexInStream2.next()
                }
            }
            logger.debug("Closing structures")
            IndexUtil.close(lexInStream1)
            IndexUtil.close(lexInStream2)
            inverted1.close()
            inverted2.close()
            invOS.close()
            destIndex.setIndexProperty("num.Documents", "" + numberOfDocuments)
            destIndex.addIndexStructure(
                "inverted",
                compressionInvertedConfig.structureClass.name,
                "org.terrier.structures.IndexOnDisk,java.lang.String,org.terrier.structures.DocumentIndex,java.lang.Class",
                "index,structureName,document," +
                        compressionInvertedConfig.postingIteratorClass.name
            )
            destIndex.addIndexStructureInputStream(
                "inverted",
                compressionInvertedConfig.structureInputStreamClass.name,
                "org.terrier.structures.IndexOnDisk,java.lang.String,java.util.Iterator,java.lang.Class",
                "index,structureName,lexicon-entry-inputstream," +
                        compressionInvertedConfig.postingIteratorClass.name
            )
            destIndex.setIndexProperty("index.inverted.fields.count", "" + fieldCount)
            lexOutStream.close()
            if (fieldCount > 0) {
                destIndex.addIndexStructure(
                    "lexicon-valuefactory",
                    FieldLexiconEntry.Factory::class.java.name, "java.lang.String", "\${index.inverted.fields.count}"
                )
            }
            destIndex.flush()
        } catch (ioe: IOException) {
            logger.error("IOException while merging lexicons and inverted files.", ioe)
        } catch (t: Throwable) {
            logger.error("Problem while merging lexicons and inverted files.", t)
            t.printStackTrace()
            throw RuntimeException(t)
        }
    }

    /**
     * Merges the two direct files and the corresponding document id files.
     */
    protected open fun mergeDirectFiles() {
        try {
            val docidOutput = DocumentIndexBuilder(destIndex, "document")
            val metaTags =
                ArrayUtils.parseCommaDelimitedString(srcIndex1.getIndexProperty("index.meta.key-names", "docno"))
            val metaTagLengths =
                ArrayUtils.parseCommaDelimitedInts(srcIndex1.getIndexProperty("index.meta.value-lengths", "20"))
            val metaReverseTags = if (MetaReverse) ArrayUtils.parseCommaDelimitedString(
                srcIndex1.getIndexProperty(
                    "index.meta.reverse-key-names",
                    ""
                )
            ) else arrayOfNulls(0)
            val metaBuilder: MetaIndexBuilder =
                CompressingMetaIndexBuilder(destIndex, metaTags, metaTagLengths, metaReverseTags)
            if (srcIndex1.getIndexProperty(
                    "index.meta.key-names",
                    "docno"
                ) != srcIndex2.getIndexProperty("index.meta.key-names", "docno")
            ) {
                metaBuilder.close()
                throw Error("Meta fields in source indices must match")
            }
            val emptyPointer: BitIndexPointer = SimpleBitIndexPointer()
            val srcFieldCount1 = srcIndex1.getIntIndexProperty("index.direct.fields.count", 0)
            val srcFieldCount2 = srcFieldCount1 // srcIndex1.getIntIndexProperty("index.direct.fields.count", 0)
            if (srcFieldCount1 != srcFieldCount2) {
                metaBuilder.close()
                throw Error("FieldCounts in source indices must match")
            }
            val fieldCount = srcFieldCount1
            for (property: String? in arrayOf("index.direct.fields.names", "index.direct.fields.count")) {
                destIndex.setIndexProperty(property, srcIndex1.getIndexProperty(property, null))
            }
            var dfOutput: AbstractPostingOutputStream? = null
            try {
                dfOutput = compressionDirectConfig.getPostingOutputStream(
                    (destIndex.path + ApplicationSetup.FILE_SEPARATOR +
                            destIndex.prefix + ".direct" + compressionDirectConfig.structureFileExtension)
                )
            } catch (e: Exception) {
                metaBuilder.close()
                throw Error("Couldn't create specified DirectInvertedOutputStream", e)
            }
            val docidInput1 = srcIndex1.getIndexStructureInputStream("document") as Iterator<DocumentIndexEntry>
            val dfInput1 = srcIndex1.getIndexStructureInputStream("direct") as PostingIndexInputStream
            val metaInput1 = srcIndex1.metaIndex
            var sourceDocid = 0
            //traversing the direct index, without any change
            while (docidInput1.hasNext()) {
                var pointerDF: BitIndexPointer? = emptyPointer
                val die = docidInput1.next()
                if (die.documentLength > 0) {
                    pointerDF = dfOutput.writePostings(dfInput1.next())
                }
                die.setBitIndexPointer(pointerDF)
                docidOutput.addEntryToBuffer(die)
                metaBuilder.writeDocumentEntry(metaInput1.getAllItems(sourceDocid))
                sourceDocid++
            }
            dfInput1.close()
            metaInput1.close()
            IndexUtil.close(docidInput1)
            val docidInput2 = srcIndex2.getIndexStructureInputStream("document") as Iterator<DocumentIndexEntry>
            val dfInput2 = srcIndex2.getIndexStructureInputStream("direct") as PostingIndexInputStream
            val metaInput2 = srcIndex2.metaIndex
            sourceDocid = 0
            while (docidInput2.hasNext()) {
                val die = docidInput2.next()
                var pointerDF: BitIndexPointer? = emptyPointer
                if (die.documentLength > 0) {
                    val postings = dfInput2.next()
                    val postingList: MutableList<Posting> = ArrayList()
                    while (postings.next() != IterablePosting.EOL) {
                        val p: Posting = postings.asWritablePosting()
                        p.id = termcodeHashmap!!.get(postings.id)
                        postingList.add(p)
                    }
                    Collections.sort(postingList, PostingIdComparator())
                    pointerDF = dfOutput.writePostings(postingList.iterator())
                }
                die.setBitIndexPointer(pointerDF)
                docidOutput.addEntryToBuffer(die)
                metaBuilder.writeDocumentEntry(metaInput2.getAllItems(sourceDocid))
                sourceDocid++
            }
            dfInput2.close()
            IndexUtil.close(docidInput2)
            metaInput2.close()
            metaBuilder.close()
            dfOutput.close()
            docidOutput.finishedCollections()
            docidOutput.close()
            compressionDirectConfig.writeIndexProperties(destIndex, "document-inputstream")
            if (fieldCount > 0) {
                destIndex.addIndexStructure(
                    "document-factory",
                    FieldDocumentIndexEntry.Factory::class.java.name,
                    "java.lang.String",
                    "\${index.direct.fields.count}"
                )
            } else {
                destIndex.addIndexStructure(
                    "document-factory",
                    BasicDocumentIndexEntry.Factory::class.java.name, "", ""
                )
            }
            destIndex.flush()
        } catch (ioe: IOException) {
            logger.error("IOException while merging df and docid files.", ioe)
        }
    }

    /**
     * Merges the two document index files, and the meta files.
     */
    protected fun mergeDocumentIndexFiles() {
        try {
            //the output docid file
            val docidOutput = DocumentIndexBuilder(destIndex, "document")
            val metaTags =
                ArrayUtils.parseCommaDelimitedString(srcIndex1.getIndexProperty("index.meta.key-names", "docno"))
            val metaTagLengths =
                ArrayUtils.parseCommaDelimitedInts(srcIndex1.getIndexProperty("index.meta.value-lengths", "20"))
            val metaReverseTags = if (MetaReverse) ArrayUtils.parseCommaDelimitedString(
                srcIndex1.getIndexProperty(
                    "index.meta.reverse-key-names",
                    ""
                )
            ) else arrayOfNulls(0)
            val metaBuilder: MetaIndexBuilder =
                CompressingMetaIndexBuilder(destIndex, metaTags, metaTagLengths, metaReverseTags)
            if (srcIndex1.getIndexProperty(
                    "index.meta.key-names",
                    "docno"
                ) != srcIndex2.getIndexProperty("index.meta.key-names", "docno")
            ) {
                metaBuilder.close()
                throw Error("Meta fields in source indices must match")
            }

            //opening the first set of files.
            val docidInput1 = srcIndex1.getIndexStructureInputStream("document") as Iterator<DocumentIndexEntry>
            val metaInput1 = srcIndex1.getIndexStructureInputStream("meta") as Iterator<Array<String>>
            var srcFieldCount1 = srcIndex1.getIntIndexProperty("index.inverted.fields.count", 0)
            val srcFieldCount2 = srcFieldCount1// srcIndex2.getIntIndexProperty("index.inverted.fields.count", 0)
            if (srcFieldCount1 != srcFieldCount2) {
                metaBuilder.close()
                throw Error("FieldCounts in source indices must match")
            }
            if (((srcIndex1.getIndexProperty(
                    "index.document-factory.class",
                    ""
                ) == "org.terrier.structures.SimpleDocumentIndexEntry\$Factory") || (srcIndex1.getIndexProperty(
                    "index.document-factory.class",
                    ""
                ) == "org.terrier.structures.BasicDocumentIndexEntry\$Factory"))
            ) {
                //for some reason, the source document index has not fields. so we shouldn't assume that fields are being used.
                srcFieldCount1 = 0
            }
            val fieldCount = srcFieldCount1

            //traversing the first set of files, without any change
            while (docidInput1.hasNext()) {
                metaInput1.hasNext()
                val die = docidInput1.next()
                val dieNew = if ((fieldCount > 0)) die else SimpleDocumentIndexEntry(die)
                docidOutput.addEntryToBuffer(dieNew)
                metaBuilder.writeDocumentEntry(metaInput1.next())
            }
            val docidInput2 = srcIndex2.getIndexStructureInputStream("document") as Iterator<DocumentIndexEntry>
            val metaInput2 = srcIndex2.getIndexStructureInputStream("meta") as Iterator<Array<String>>
            //traversing the 2nd set of files, without any change
            while (docidInput2.hasNext()) {
                metaInput2.hasNext()
                val die = docidInput2.next()
                val dieNew = if ((fieldCount > 0)) die else SimpleDocumentIndexEntry(die)
                docidOutput.addEntryToBuffer(dieNew)
                metaBuilder.writeDocumentEntry(metaInput2.next())
            }
            docidOutput.finishedCollections()
            docidOutput.close()
            metaBuilder.close()
            IndexUtil.close(docidInput1)
            IndexUtil.close(docidInput2)
            IndexUtil.close(metaInput1)
            IndexUtil.close(metaInput2)
//            destIndex.setIndexProperty("index.inverted.fields.count", ""+ fieldCount);
            if (fieldCount > 0) {
                destIndex.addIndexStructure(
                    "document-factory",
                    FieldDocumentIndexEntry.Factory::class.java.name,
                    "java.lang.String",
                    "\${index.inverted.fields.count}"
                )
            } else {
                destIndex.addIndexStructure(
                    "document-factory",
                    SimpleDocumentIndexEntry.Factory::class.java.name, "", ""
                )
            }
            destIndex.flush()
        } catch (ioe: IOException) {
            logger.error("IOException while merging docid files.", ioe)
        }
    }

    /**
     * creates the final term code to offset file, and the lexicon hash if enabled.
     */
    protected fun createLexidFile() {
        LexiconBuilder.optimise(destIndex, "lexicon")
    }

    fun setReverseMeta(value: Boolean) {
        MetaReverse = value
    }

    /**
     * Merges the structures created by terrier.
     */
    fun mergeStructures() {
        val bothInverted = srcIndex1.hasIndexStructure("inverted") && srcIndex2.hasIndexStructure("inverted")
        val bothDirect = srcIndex1.hasIndexStructure("direct") && srcIndex2.hasIndexStructure("direct")
        val bothLexicon = srcIndex1.hasIndexStructure("lexicon") && srcIndex2.hasIndexStructure("lexicon")
        val bothDocument = srcIndex1.hasIndexStructure("document") && srcIndex2.hasIndexStructure("document")
        val t1 = System.currentTimeMillis()
        keepTermCodeMap = bothDirect
        var t2: Long = 0
        var t3: Long = 0
        var t4: Long = 0
        if (bothInverted) {
            mergeInvertedFiles()
            t2 = System.currentTimeMillis()
            logger.info("merged inverted files in " + ((t2 - t1) / 1000.0))
        } else if (bothLexicon) {
            LexiconMerger(srcIndex1, srcIndex2, destIndex).mergeLexicons()
            t2 = System.currentTimeMillis()
            logger.info("merged lexicons in " + ((t2 - t1) / 1000.0))
        } else {
            logger.warn("No inverted or lexicon - no merging of lexicons took place")
            t2 = System.currentTimeMillis()
        }
        if (bothInverted || bothLexicon) {
            createLexidFile()
            t3 = System.currentTimeMillis()
            logger.debug("created lexid file and lex hash in " + ((t3 - t2) / 1000.0))
        }
        t3 = System.currentTimeMillis()
        if ((!bothDirect || (ApplicationSetup.getProperty("merge.direct", "true") == "false")) && bothDocument) {
            mergeDocumentIndexFiles()
            t4 = System.currentTimeMillis()
            logger.info("merged documentindex files in " + ((t4 - t3) / 1000.0))
        } else if (!bothDocument) {
            t2 = System.currentTimeMillis()
            throw IllegalArgumentException("No document - no merging of document or meta structures took place")
        } else {
            mergeDirectFiles()
            t4 = System.currentTimeMillis()
            logger.info("merged direct files in " + ((t4 - t3) / 1000.0))
        }
//        destIndex.setIndexProperty("index.inverted.fields.count", ""+ fieldCount)
//        destIndex.setIndexProperty("index.inverted.fields.count", ""+ fieldCount)
        if (keepTermCodeMap) {
            //save up some memory
            termcodeHashmap!!.clear()
            termcodeHashmap = null
        }
    }

    companion object {
        /** the logger used  */
        protected val logger = LoggerFactory.getLogger(StructureMergerCustom::class.java)
        protected fun getInterfaces(o: Any): Array<Class<*>> {
            val list: MutableList<Class<*>> = ArrayList()
            var c: Class<*> = o.javaClass
            while (c != Any::class.java) {
                for (i: Class<*> in c.interfaces) {
                    list.add(i)
                }
                c = c.superclass
            }
            return list.toTypedArray()
        }
    }

    /**
     * constructor
     * @param _srcIndex1
     * @param _srcIndex2
     * @param _destIndex
     */
    init {
        srcIndex2 = srcIndex2
        destIndex = _destIndex
        numberOfDocuments = 0
        numberOfPointers = 0
        numberOfTerms = 0
        val srcFieldCount1 = srcIndex1.getIntIndexProperty("index.inverted.fields.count", 0)
        val srcFieldCount2 = srcIndex2.getIntIndexProperty("index.inverted.fields.count", 0)
        if (srcFieldCount1 != srcFieldCount2) {
            throw Error("FieldCounts in source indices must match")
        }
        val fieldNames =
            ArrayUtils.parseCommaDelimitedString(srcIndex1.getIndexProperty("index.inverted.fields.names", ""))
        assert(srcFieldCount1 == fieldNames.size)
        fieldCount = srcFieldCount1
        compressionDirectConfig = CompressionFactory.getCompressionConfiguration("direct", fieldNames, 0, 0)
        compressionInvertedConfig = CompressionFactory.getCompressionConfiguration("inverted", fieldNames, 0, 0)
    }
}

