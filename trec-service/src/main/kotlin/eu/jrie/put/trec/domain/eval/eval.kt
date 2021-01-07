package eu.jrie.put.trec.domain.eval

import java.io.File
import java.lang.ProcessBuilder.Redirect.PIPE
import java.util.*
import java.util.concurrent.TimeUnit.SECONDS

private data class Rel (
        val topicId: String,
        val documentId: String,
        val relevant: Boolean
)

data class EvaluationData (
        val topicId: Int,
        val documentId: Int,
        val rank: Int,
        val score: Float
)

class TrecEvalException (msg: String) : Exception(msg)

data class EvalResult (
    val name: String,
    val value: String
)

private val WORKDIR = File("/")
private val RESULT_DELIMITER_REGEX = Regex("[ \t]+")

class EvaluationService {

    fun evaluate(runName: String, qrelsSet: String, data: List<EvaluationData>): Triple<List<EvalResult>, String, String> {
        val (results, log) = runEvaluation(runName, qrelsSet, data)
        val latex = """
            \begin{tabular}{ |c|c| } 
             \hline
             ${results.joinToString { "${it.name} & ${it.value} \\\\\n" }}
             \hline
            \end{tabular}
        """.trimIndent()
        return Triple(results, log, latex)
    }

    private fun runEvaluation(name: String, qrelsSet: String, data: List<EvaluationData>): Pair<List<EvalResult>, String> {
        val dataFile = File("/data_${UUID.randomUUID()}")
        data.asSequence()
            .map { "${it.topicId} Q0 ${it.documentId} ${it.rank} ${it.score} $name\n" }
            .forEach { dataFile.appendText(it) }

        val cmd = "/trec_eval/trec_eval -m all_trec /qrels/$qrelsSet ${dataFile.absolutePath}".split(" ")
        val evaluation = ProcessBuilder(cmd)
            .directory(WORKDIR)
            .redirectOutput(PIPE)
            .redirectError(PIPE)
            .start()
            .apply { waitFor(30, SECONDS) }

        dataFile.delete()

        evaluation.errorStream
            .bufferedReader()
            .readText()
            .let {
                if (it.isNotBlank()) throw TrecEvalException("Error during eval: $it")
            }

        val log = evaluation.inputStream
            .bufferedReader()
            .readText()
        val results = log.lineSequence()
            .drop(1)
            .map { it.replace(RESULT_DELIMITER_REGEX, " ") }
            .map { it.split(" ") }
            .filter { it.size >= 3 }
            .map { (name, _, value) -> EvalResult(name, value) }
            .toList()

        return results to log
    }
}
