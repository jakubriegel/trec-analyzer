package eu.jrie.put.trec.infra

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File

fun env(name: String): String = System.getenv(name)

val jsonMapper = JsonMapper().registerKotlinModule()

val config: Config = ConfigFactory.parseFile(File("/application.conf"))
