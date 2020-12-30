package eu.jrie.put.trec

import eu.jrie.put.trec.api.startServer
import eu.jrie.put.trec.domain.index.initEs
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
@FlowPreview
fun main() {
    app(initIndexes = true, startServer = true)
}


@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
@FlowPreview
fun app(initIndexes: Boolean, startServer: Boolean) {
    if (initIndexes) initIndexes()
    if (startServer) startServer()
}

@ExperimentalCoroutinesApi
fun initIndexes() {
//    initTerrier()
    initEs()
}
