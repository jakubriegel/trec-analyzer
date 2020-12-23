package eu.jrie.put.trec.api

import eu.jrie.put.trec.domain.eval.TrecEvalException
import eu.jrie.put.trec.domain.eval.evaluate
import eu.jrie.put.trec.domain.eval.validate
import eu.jrie.put.trec.domain.index.es.ElasticsearchRepository
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
import org.apache.http.HttpStatus


@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
fun startServer() {
    embeddedServer(Netty, port = 8001) {
        install(ContentNegotiation) {
            register(ContentType.Application.Json, JacksonConverter())
        }

        val esContext = newCoroutineContext(newFixedThreadPoolContext(1, "esContext"))
        val elasticsearchRepository = ElasticsearchRepository(esContext)

        routing {
            post("/find") {
                environment.log.info("find")
                val request: QueryRequest = call.receive()
                val results = elasticsearchRepository.find(request.query, request.options.algorithm)

                environment.log.info("find ok")
                call.respond(
                    QueryResponse(request, results)
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
                try {
                    val results = evaluate(request.name, request.data)
                    call.respond(results)
                } catch (e: TrecEvalException) {
                    call.respond(HttpStatusCode.InternalServerError, e.message as Any)
                }
            }

            post("/evaluate/search") {

            }
        }
    }.start(wait = true)
}