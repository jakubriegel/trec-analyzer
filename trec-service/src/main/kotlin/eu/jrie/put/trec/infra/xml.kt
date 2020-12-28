package eu.jrie.put.trec.infra

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import eu.jrie.put.trec.domain.Article
import eu.jrie.put.trec.domain.MeshHeading
import eu.jrie.put.trec.domain.query.Queries
import eu.jrie.put.trec.domain.query.Query

data class Articles(
    val data: Sequence<Article>
)

class ArticlesDeserializer : StdDeserializer<Articles>(Articles::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Articles {
        return p.codec.readTree<JsonNode>(p)
            .get("PubmedArticle")
            .elements()
            .asSequence()
            .map { it.articleValue() }
            .let { Articles(it) }
    }

    private fun JsonNode.articleValue(): Article {
        val id = get("MedlineCitation").get("PMID").get("").textValue().toInt()
        val title = get("MedlineCitation").get("Article").get("ArticleTitle").textValue()
        val abstract = get("MedlineCitation").get("Article")?.get("Abstract")?.get("AbstractText")?.textValue() ?: ""
        val keywords = get("MedlineCitation")?.get("KeywordList")?.get("Keyword")
            ?.elements()
            ?.asSequence()
            ?.map { it.get("") }
            ?.filterNotNull()
            ?.map { it.textValue() }
            ?.toList() ?: emptyList()
        val meshHeadings = get("MedlineCitation").get("MeshHeadingList").get("MeshHeading")
            .elements()
            .asSequence()
            .map { it.get("DescriptorName") to it.get("QualifierName") }
            .map { (description, name) -> description?.get("")?.textValue() to name?.get("")?.textValue() }
            .map { (description, name) -> MeshHeading(description ?: "", name ?: "") }
            .toList()

        return Article(id, title, abstract, keywords, meshHeadings)
    }
}

class QueriesDeserializer : StdDeserializer<Queries>(Queries::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Queries {
        return p.codec.readTree<JsonNode>(p)
            .get("topic")
            .elements()
            .asSequence()
            .map { it.queryValue() }
            .let { Queries(it) }
    }

    private fun JsonNode.queryValue(): Query {
        val id = get("number").textValue().toInt()
        val disease = get("disease").textValue()
        val gene = get("gene").textValue()
        val treatment = get("treatment").textValue()
        return Query(id, disease, gene, treatment)
    }
}

val xmlMapper = XmlMapper().apply {
    val module = SimpleModule().apply {
        addDeserializer(Articles::class.java, ArticlesDeserializer())
        addDeserializer(Queries::class.java, QueriesDeserializer())
    }
    registerModule(module)
}

