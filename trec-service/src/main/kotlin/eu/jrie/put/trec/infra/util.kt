package eu.jrie.put.trec.infra

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

object Resources {
    fun get(name: String): String = javaClass.getResource(name).path
}

val jsonMapper = JsonMapper().registerKotlinModule()
