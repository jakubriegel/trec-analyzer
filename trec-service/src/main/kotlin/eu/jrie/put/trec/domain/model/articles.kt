package eu.jrie.put.trec.domain.model

data class Articles(
    val data: Sequence<Article>
)

data class Article(
    val id: Int,
    val title: String,
    val abstract: String,
    val keywords: List<String>,
    val meshHeadings: List<MeshHeading>
)

data class MeshHeading(
    val description: String,
    val name: String
)
