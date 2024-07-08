plugins {
    kotlin("jvm") version "1.9.22"
}

group = "gb.tech"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.azure:azure-messaging-servicebus:7.13.3")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}