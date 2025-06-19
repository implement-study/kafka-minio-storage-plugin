import com.shengsheng.kafka.storage.build.CreateTopicTask
import com.shengsheng.kafka.storage.build.ProducerTask

plugins {
    id("java-library")
    id("com.github.johnrengelman.shadow").version("8.1.1")
}

group = "com.shengsheng"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}


val classPath = "$buildDir/libs"

val copyMinioJar by tasks.registering(Copy::class) {
    dependsOn("jar")
    from(configurations.runtimeClasspath.get())
    into(classPath)
}

tasks.register("createTieredTopic", CreateTopicTask::class) {
    group = "tiered"
    description = "create topic for tiered storage"
}


tasks.register("produceData", ProducerTask::class) {
    group = "tiered"
    description = "producer seq message to tiered topic"
}

tasks.register("buildMinioPlugin") {
    group = "tiered"
    description = "build jar and print config"
    dependsOn("jar")
    dependsOn("copyMinioJar")
    doLast {
        println(
            """
        remote.log.storage.system.enable=true
        remote.log.metadata.manager.listener.name=PLAINTEXT
        remote.log.storage.manager.class.name=com.shengsheng.kafka.storage.minio.MinioRemoteStorageManager
        remote.log.storage.manager.class.path=${classPath}/*
        remote.log.storage.manager.impl.prefix=rsm.config.
        remote.log.metadata.manager.impl.prefix=rlmm.config.
        rsm.config.minio.endpoint=http://localhost:9000
        rsm.config.minio.username=minioadmin
        rsm.config.minio.password=minioadmin
        rsm.config.minio.kafka.bucket=kafka-data
        rlmm.config.remote.log.metadata.topic.replication.factor=1
        log.retention.check.interval.ms=1000
    """.trimIndent()
        )
        println()
        println(
            "------------------------ you can run this command to format the metadata ---------------"
        )
        println("")
        println("uuid=\$(bin/kafka-storage.sh random-uuid)")
        println("bin/kafka-storage.sh format --standalone -t \$uuid -c config/server.properties")
        println("bin/kafka-server-start.sh config/server.properties")

    }
}


dependencies {
    compileOnly("org.slf4j:slf4j-api:2.0.17")
    compileOnly("org.apache.kafka:kafka-storage-api:4.0.0")
    compileOnly("org.apache.kafka:kafka-clients:4.0.0")
    implementation("io.minio:minio:8.5.17")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}
