package eu.jrie.put.trec.infra

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.typesafe.config.ConfigFactory
import java.io.File

object Resources {
    fun get(name: String): String = javaClass.getResource(name).path
}

val jsonMapper = JsonMapper().registerKotlinModule()

val config = ConfigFactory.parseFile(File("/application.conf"))
