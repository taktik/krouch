/*
 *    Copyright 2020 Taktik SA
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val repoUsername: String by project
val repoPassword: String by project
val mavenReleasesRepository: String by project

plugins {
    kotlin("jvm") version "1.4.21"
    kotlin("kapt") version "1.4.21"
}

buildscript {
    repositories {
        mavenCentral()
        maven { url = uri("https://maven.taktik.be/content/groups/public") }
    }
    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.4.21")
        classpath("org.jetbrains.kotlin:kotlin-allopen:1.4.21")
        classpath("com.taktik.gradle:gradle-plugin-git-version:1.0.13")
    }
    plugins {
        `maven-publish`
    }
}

apply(plugin = "git-version")

val gitVersion: String? by project
group = "org.taktik.couchdb"
version = gitVersion ?: "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11

    withSourcesJar()
}

repositories {
    mavenCentral()
    jcenter()
    maven {
        url = uri("https://maven.taktik.be/content/groups/public")
    }
}

apply(plugin = "kotlin")
apply(plugin = "maven-publish")

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}

tasks.withType<PublishToMavenRepository> {
    doFirst {
        println("Artifact >>> ${project.group}:${project.name}:${project.version} <<< published to Maven repository")
    }
}

dependencies {
    api("com.github.pozo:mapstruct-kotlin:1.3.1.2")
    kapt("com.github.pozo:mapstruct-kotlin-processor:1.3.1.2")

    implementation(group = "io.icure", name = "async-jackson-http-client", version = "0.1.11-5b7f5b35ee")
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.12.5")
    implementation(group = "com.fasterxml.jackson.module", name = "jackson-module-kotlin", version = "2.12.5")

    implementation(group = "org.jetbrains.kotlin", name = "kotlin-stdlib", version = "1.5.21")
    implementation(group = "org.jetbrains.kotlin", name = "kotlin-reflect", version = "1.5.21")
    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-coroutines-core", version = "1.5.2")
    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-coroutines-reactor", version = "1.5.2")
    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-collections-immutable-jvm", version = "0.3.4")

    // Logging
    implementation(group = "ch.qos.logback", name = "logback-classic", version = "1.2.6")
    implementation(group = "ch.qos.logback", name = "logback-access", version = "1.2.6")
    implementation(group = "org.slf4j", name = "slf4j-api", version = "1.7.32")
    implementation(group = "org.slf4j", name = "jul-to-slf4j", version = "1.7.32")
    implementation(group = "org.slf4j", name = "jcl-over-slf4j", version = "1.7.32")
    implementation(group = "org.slf4j", name = "log4j-over-slf4j", version = "1.7.32")

    implementation(group = "com.google.guava", name = "guava", version = "30.1.1-jre")
    implementation(group = "org.apache.httpcomponents", name = "httpclient", version = "4.5.13")
    implementation(group = "io.projectreactor", name = "reactor-core", version = "3.4.10")
    implementation(group = "io.projectreactor.netty", name = "reactor-netty", version = "1.0.11")

    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter", version = "5.8.0")
}

publishing {
    publications {
        create<MavenPublication>("krouch") {
            from(components["java"])
        }
    }

    repositories {
        maven {
            name = "Taktik"
            url = uri(mavenReleasesRepository)
            credentials {
                username = repoUsername
                password = repoPassword
            }
        }
    }
}
