package eu.jrie.put.trec.api

import eu.jrie.put.trec.api.IndexEngine.ELASTICSEARCH
import eu.jrie.put.trec.domain.eval.EvaluationData
import eu.jrie.put.trec.domain.eval.TrecEvalException
import eu.jrie.put.trec.domain.eval.evaluate
import eu.jrie.put.trec.domain.eval.validate
import eu.jrie.put.trec.domain.index.ElasticsearchRepository
import eu.jrie.put.trec.domain.query.Query
import eu.jrie.put.trec.domain.query.QueryRepository
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.jackson.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.newFixedThreadPoolContext


@FlowPreview
@ObsoleteCoroutinesApi
fun startServer() {
    embeddedServer(Netty, port = 8001) {
        install(CallLogging)
        install(ContentNegotiation) {
            register(ContentType.Application.Json, JacksonConverter())
        }

        val esContext = newFixedThreadPoolContext(3, "esContext")
//        val terrierContext = newFixedThreadPoolContext(3, "terrierContext")

        val repositories = mapOf(
            ELASTICSEARCH to ElasticsearchRepository(esContext),
//            TERRIER to TerrierRepository(terrierContext)
        )

        val queryRepository = QueryRepository()
        fun Query.asText() = "$disease $gene $treatment"

        routing {
            post("/find") {
                val request: CustomQueryRequest = call.receive()
                val results = repositories.getValue(request.options.engine).find(request.query, request.options.algorithm)
                call.respond(
                    CustomQueryResponse(request, results)
                )
            }

            post("/find/query") {
                val request: QueryRequest = call.receive()
                val topic = queryRepository.get(request.queryId)
                    .also { environment.log.info("Query ${request.queryId} resolved as $it.") }
                val results = repositories.getValue(request.options.engine).find(topic.asText(), request.options.algorithm)
                call.respond(
                    QueryResponse(topic, request.options, results)
                )
            }

            post("/validate") {
                val documentId = call.request.queryParameters["documentId"]!!
                val queryId = call.request.queryParameters["queryId"]!!
                val isRelevant = call.request.queryParameters["isRelevant"]!!.let { it == "1" }

                call.respond(validate(queryId, documentId) ?: "404")
            }

            post("/evaluate") {
                val request: EvaluationRequest = call.receive()
                request.queriesIds
                    .asFlow()
                    .map { queryRepository.get(it) }
                    .map { it.id to repositories.getValue(request.options.engine).find(it.asText(), request.options.algorithm) }
                    .flatMapMerge { (queryId, matches) ->
                        matches.asFlow()
                            .withIndex()
                            .map { (rank, match) ->
                                EvaluationData(queryId, match.article.id, rank, match.score)
                            }
                    }
                    .toList()
                    .let { data ->
                        try {
                            val (results, log) = evaluate(request.name, data)
                            val latex = """
                                \begin{tabular}{ |c|c| } 
                                 \hline
                                 ${results.joinToString { "${it.name} & ${it.value} \\\\\n" }}
                                 \hline
                                \end{tabular}
                            """.trimIndent()

                            call.respond(EvaluationResponse(results, log, latex))
                        } catch (e: TrecEvalException) {
                            call.respond(InternalServerError, e.message as Any)
                        }
                    }
            }
        }
    }.start(wait = true)
}
