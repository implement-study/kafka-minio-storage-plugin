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


val buildMinioPlugin by tasks.registering(Copy::class) {
    dependsOn("jar")
    dependsOn("copyMinioJar")
    println("""
        remote.log.storage.system.enable=true
        remote.log.metadata.manager.listener.name=PLAINTEXT
        remote.log.storage.manager.class.name=com.shengsheng.kafka.storage.minio.MinioRemoteStorageManager
        remote.log.storage.manager.class.path=${classPath}/*
        remote.log.storage.manager.impl.prefix=rsm.config.
        remote.log.metadata.manager.impl.prefix=rlmm.config.
        rlmm.config.minio.endpoint=http://localhost:9000
        rlmm.config.remote.log.metadata.topic.replication.factor=1
        log.retention.check.interval.ms=1000
    """.trimIndent())
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
