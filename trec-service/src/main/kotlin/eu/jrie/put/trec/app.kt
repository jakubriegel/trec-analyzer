package eu.jrie.put.trec

import eu.jrie.put.trec.api.startServer
import eu.jrie.put.trec.domain.index.es.initEs
import eu.jrie.put.trec.domain.index.terrier.initTerrier
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi

@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@FlowPreview
fun main() {
    app(initIndexes = true, startServer = true)
}


@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@FlowPreview
fun app(initIndexes: Boolean, startServer: Boolean) {
    if (initIndexes) initIndexes()
    if (startServer) startServer()
}

fun initIndexes() {
    initTerrier()
    initEs()
}
