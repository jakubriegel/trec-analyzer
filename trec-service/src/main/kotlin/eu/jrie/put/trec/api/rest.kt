package eu.jrie.put.trec.api

import eu.jrie.put.trec.domain.eval.EvaluationData
import eu.jrie.put.trec.domain.eval.EvaluationService
import eu.jrie.put.trec.domain.eval.TrecEvalException
import eu.jrie.put.trec.domain.index.ArticleMatch
import eu.jrie.put.trec.domain.index.IndexService
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
import io.ktor.util.pipeline.*
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList


@FlowPreview
@ObsoleteCoroutinesApi
fun startServer() {
    embeddedServer(Netty, port = 8001) {

        install(CallLogging)
        install(ContentNegotiation) {
            register(ContentType.Application.Json, JacksonConverter())
        }

        val indexService = IndexService()
        val evaluationService = EvaluationService()

        routing {
            route("/find") {
                post {
                    val request: CustomQueryRequest = call.receive()

                    val matches = indexService.find(
                        query = request.query,
                        engine = request.options.engine,
                        algorithm = request.options.algorithm
                    ).take(10)

                    call.respond(
                        CustomQueryResponse(request, matches.toList())
                    )
                }

                post("/topic") {
                    val request: QueryRequest = call.receive()

                    val (topic, matches) = indexService.findByTopic(
                        topicId = request.topic.id,
                        topicSet = request.topic.set,
                        engine = request.options.engine,
                        algorithm = request.options.algorithm
                    )

                    call.respond(
                        QueryResponse(topic, request.options, matches.toList())
                    )
                }
            }

            route("/evaluate") {
                get("/qrels") {
                    val documentId = call.request.queryParameters["documentId"]!!.toInt()
                    val topicId = call.request.queryParameters["topicId"]!!.toInt()

                    val result = evaluationService.validate(topicId, documentId)
                    call.respond(EvaluateQrelsResponse(documentId, topicId, result))
                }

                route("/topics") {
                    suspend fun PipelineContext<Unit, ApplicationCall>.handleEvaluateTopics(
                        runName: String,
                        matchesData: Flow<Pair<Int, Flow<ArticleMatch>>>
                    ) {
                        val data = matchesData.flatMapMerge { (topicId, matches) ->
                            matches.map {
                                EvaluationData(topicId, it.article.id, it.rank, it.score)
                            }
                        }.toList()

                        try {
                            val (results, log, latex) = evaluationService.evaluate(runName, data)
                            call.respond(EvaluateTopicsResponse(results, log, latex))
                        } catch (e: TrecEvalException) {
                            call.respond(InternalServerError, e.message as Any)
                        }
                    }

                    post {
                        val request: EvaluateTopicsRequest = call.receive()

                        val matches = request.topics
                            .asFlow()
                            .map { indexService.findByTopic(
                                topicId = it.id,
                                topicSet = it.set,
                                engine = request.options.engine,
                                algorithm = request.options.algorithm
                            ) }
                            .map { (query, matches) -> query.id to matches }

                        handleEvaluateTopics(request.name, matches)
                    }

                    post("/all") {
                        val request: EvaluateAllTopicsRequest = call.receive()

                        val matches = indexService.findForAllTopics(
                            topicSet = request.topicSet,
                            engine = request.options.engine,
                            algorithm = request.options.algorithm
                        )

                        handleEvaluateTopics(request.name, matches)
                    }
                }
            }
        }
    }.start(wait = true)
}
