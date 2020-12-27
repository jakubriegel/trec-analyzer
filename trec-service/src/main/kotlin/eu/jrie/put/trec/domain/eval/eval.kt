package eu.jrie.put.trec.domain.eval

import java.io.File
import java.lang.ProcessBuilder.Redirect.PIPE
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

private data class Rel (
        val queryId: String,
        val documentId: String,
        val relevant: Boolean
)

data class EvaluationData (
        val queryId: Int,
        val documentId: Int,
        val rank: Int,
        val score: Float
)

class TrecEvalException (msg: String) : Exception(msg)

private const val QRELS_FILE_PATH = "/qrels/qrels2020"
private val WORKDIR = File("/")

private val qrels = File(QRELS_FILE_PATH)
    .readLines()
    .map { it.split(" ") }
    .map { (queryId, _, documentId, isRelevant) ->
        Rel(queryId, documentId, isRelevant != "0")
    }

fun validate(queryId: String, documentId: String): Boolean? =
    qrels.find { it.queryId == queryId && it.documentId == documentId }?.relevant

fun evaluate(name: String, data: List<EvaluationData>): String {
    val dataFile = File("/data_${UUID.randomUUID()}")
    data.asSequence()
        .map { "${it.queryId} Q0 ${it.documentId} ${it.rank} ${it.score} $name\n" }
        .forEach { dataFile.appendText(it) }

    val cmd = "/trec_eval/trec_eval -m all_trec $QRELS_FILE_PATH ${dataFile.absolutePath}".split(" ")
    val evaluation = ProcessBuilder(cmd)
        .directory(WORKDIR)
        .redirectOutput(PIPE)
        .redirectError(PIPE)
        .start()
        .apply { waitFor(30, SECONDS) }

    dataFile.delete()

    val err = evaluation.errorStream.bufferedReader().readText()
    if (err.isNotBlank()) {
        throw TrecEvalException("Error during eval: $err")
    } else {
        return evaluation.inputStream.bufferedReader().readText()
    }
}
