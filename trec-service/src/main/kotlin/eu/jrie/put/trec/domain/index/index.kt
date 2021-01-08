package eu.jrie.put.trec.domain.index

import eu.jrie.put.trec.api.IndexAlgorithm
import eu.jrie.put.trec.api.IndexEngine
import eu.jrie.put.trec.api.IndexEngine.ELASTICSEARCH
import eu.jrie.put.trec.api.IndexEngine.TERRIER
import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.query.Topic
import eu.jrie.put.trec.domain.query.TopicRepository
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.newFixedThreadPoolContext

data class ArticleMatch (
    val rank: Int,
    val score: Float,
    val article: Article
)

@ObsoleteCoroutinesApi
class IndexService {
    private val esContext = newFixedThreadPoolContext(3, "esContext")
    private val terrierContext = newFixedThreadPoolContext(3, "terrierContext")

    private val repositories = mapOf(
        ELASTICSEARCH to ElasticsearchRepository(esContext),
        TERRIER to TerrierRepository(terrierContext)
    )

    private val topicRepository = TopicRepository()

    suspend fun find(query: String, engine: IndexEngine, algorithm: IndexAlgorithm): Flow<ArticleMatch> {
        return repositories.getValue(engine)
            .find(query, algorithm)
    }

    private suspend fun find(topic: Topic, engine: IndexEngine, algorithm: IndexAlgorithm): Flow<ArticleMatch> {
        return find(topic.asText(), engine, algorithm)
    }

    suspend fun findByTopic(topicId: Int, topicSet: String, engine: IndexEngine, algorithm: IndexAlgorithm): Pair<Topic, Flow<ArticleMatch>> {
        val topic = topicRepository.get(topicId, topicSet)
        return topic to find(topic.asText(), engine, algorithm)
    }

    @FlowPreview
    suspend fun findForAllTopics(topicSet: String, engine: IndexEngine, algorithm: IndexAlgorithm): Flow<Pair<Int, Flow<ArticleMatch>>> {
        return topicRepository.getAll(topicSet)
            .asFlow()
            .map { it.id to find(it, engine, algorithm) }
    }

    private companion object {
        fun Topic.asText() = "$disease $gene $treatment"
    }
}
