plugins {
    id("java-library")
}


repositories {
    mavenCentral()
}


dependencies {
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}
