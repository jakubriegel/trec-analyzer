plugins {
    kotlin("jvm") version "1.4.21"
    id("com.github.johnrengelman.shadow") version "5.2.0"
}

group = "eu.jrie.put.trec"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val ktorVersion = "1.5.1"

dependencies {
    // kotlin
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))

    // ktor
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-jackson:$ktorVersion")

    // jackson
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.12.1")

    // elastic
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:7.10.1")

    // terrier
    implementation("org.terrier:terrier-core:5.4")
    implementation("org.terrier:terrier-batch-indexers:5.4") {
        exclude("com.github.cmacdonald")
    }

    // logger
    implementation("org.apache.logging.log4j:log4j-core:2.14.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")
}

tasks.shadowJar {
    manifest {
        attributes(
            mapOf(
                "Main-Class" to "eu.jrie.put.trec.AppKt"
            )
        )
    }
    isZip64 = true
    archiveFileName.set("trec-service.jar")
}
