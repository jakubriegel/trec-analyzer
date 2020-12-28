package eu.jrie.put.trec

import eu.jrie.put.trec.api.startServer
import eu.jrie.put.trec.domain.index.initEs
import eu.jrie.put.trec.domain.index.initTerrier
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi

@ObsoleteCoroutinesApi
@FlowPreview
fun main() {
    app(initIndexes = true, startServer = true)
}


@ObsoleteCoroutinesApi
@FlowPreview
fun app(initIndexes: Boolean, startServer: Boolean) {
    if (initIndexes) initIndexes()
    if (startServer) startServer()
}

fun initIndexes() {
    initTerrier()
    initEs()
}
