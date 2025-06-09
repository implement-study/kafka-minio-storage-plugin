plugins {
    id("java-library")
    id("com.github.johnrengelman.shadow").version("8.1.1")
}

group = "com.shengsheng"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val copyMinioJar by tasks.registering(Copy::class) {
    dependsOn("jar")
    from(configurations.runtimeClasspath.get())
    into("$buildDir/libs/libs")
}


dependencies {
    implementation("org.apache.kafka:kafka-storage-api:4.0.0")
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    implementation("io.minio:minio:8.5.17")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}
