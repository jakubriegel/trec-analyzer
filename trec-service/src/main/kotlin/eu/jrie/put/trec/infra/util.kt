package eu.jrie.put.trec

object Resources {
    fun get(name: String): String = javaClass.getResource(name).path
}
