package eu.jrie.put.trec.api

import eu.jrie.put.trec.api.IndexEngine.ELASTICSEARCH
import eu.jrie.put.trec.api.IndexEngine.TERRIER
import eu.jrie.put.trec.domain.eval.EvaluationData
import eu.jrie.put.trec.domain.eval.TrecEvalException
import eu.jrie.put.trec.domain.eval.evaluate
import eu.jrie.put.trec.domain.eval.validate
import eu.jrie.put.trec.domain.index.es.ElasticsearchRepository
import eu.jrie.put.trec.domain.index.terrier.TerrierRepository
import eu.jrie.put.trec.domain.query.QueryRepository
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*


@FlowPreview
@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
fun startServer() {
    embeddedServer(Netty, port = 8001) {
        install(ContentNegotiation) {
            register(ContentType.Application.Json, JacksonConverter())
        }

        val esContext = newCoroutineContext(newFixedThreadPoolContext(3, "esContext"))
        val elasticsearchRepository = ElasticsearchRepository(esContext)

        val terrierContext = newCoroutineContext(newFixedThreadPoolContext(3, "terrierContext"))
        val terrierRepository = TerrierRepository(terrierContext)

        val queryRepository = QueryRepository()

        routing {
            post("/find") {
                environment.log.info("find")
                val request: CustomQueryRequest = call.receive()

                val results = when (request.options.engine) {
                    ELASTICSEARCH -> elasticsearchRepository.find(request.query, request.options.algorithm)
                    TERRIER -> terrierRepository.find(request.query, request.options.algorithm)
                }


                call.respond(
                    CustomQueryResponse(request, results)
                )
            }

            post("/find/query") {
                environment.log.info("find query")
                val request: QueryRequest = call.receive()

                val query = queryRepository.get(request.queryId)
                val results = elasticsearchRepository.find(query.disease, request.options.algorithm)

                call.respond(
                    QueryResponse(query, request.options, results)
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
                    .map { it.id to elasticsearchRepository.find(it.disease, request.options.algorithm) }
                    .flatMapMerge { (queryId, matches) ->
                        matches.asFlow()
                            .withIndex()
                            .map { (rank, match) ->
                                EvaluationData(queryId, match.article.id, rank, match.score)
                            }
                    }
                    .toList()
                    .let {
                        try {
                            val results = evaluate(request.name, it)
                            call.respond(results)
                        } catch (e: TrecEvalException) {
                            call.respond(HttpStatusCode.InternalServerError, e.message as Any)
                        }
                    }
            }
        }
    }.start(wait = true)
}