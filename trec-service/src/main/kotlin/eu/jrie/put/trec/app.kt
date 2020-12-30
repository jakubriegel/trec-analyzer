package eu.jrie.put.trec

import eu.jrie.put.trec.api.startServer
import eu.jrie.put.trec.domain.index.initEs
import eu.jrie.put.trec.infra.env
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
@FlowPreview
fun main() {
    app()
}


@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
@FlowPreview
fun app() {
    if (env("TREC_INIT_INDEX") == "1") initIndexes()
    if (env("TREC_SERVER") == "1") startServer()
}

@ExperimentalCoroutinesApi
fun initIndexes() {
//    initTerrier()
    initEs()
}
