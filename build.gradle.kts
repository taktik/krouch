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
val mavenRepository: String by project

plugins {
    kotlin("jvm") version "1.4.10"
    kotlin("kapt") version "1.4.10"
}

buildscript {
    repositories {
        mavenCentral()
        maven { url = uri("https://maven.taktik.be/content/groups/public") }
    }
    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.4.10")
        classpath("org.jetbrains.kotlin:kotlin-allopen:1.4.10")
        classpath("com.taktik.gradle:gradle-plugin-git-version:1.0.13")
    }
}

apply(plugin = "git-version")

val gitVersion: String? by project
group = "org.taktik.couchdb"
version = gitVersion ?: "0.0.1-SNAPSHOT"

java.sourceCompatibility = JavaVersion.VERSION_11
java.targetCompatibility = JavaVersion.VERSION_11

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

dependencies {
    api("com.github.pozo:mapstruct-kotlin:1.3.1.2")
    kapt("com.github.pozo:mapstruct-kotlin-processor:1.3.1.2")

    implementation(group = "com.fasterxml.jackson.module", name = "jackson-module-kotlin", version ="2.11.1")
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version ="2.11.1")
    implementation(group = "com.fasterxml.jackson.dataformat", name = "jackson-dataformat-smile", version = "2.11.1")
    implementation(group = "org.jetbrains.kotlin", name = "kotlin-stdlib-jdk8")
    implementation(group = "org.jetbrains.kotlin", name = "kotlin-reflect")
    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-coroutines-core", version = "1.4.2")
    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-coroutines-reactor", version = "1.4.2")
    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-collections-immutable-jvm", version = "0.3")

    // Logging
    implementation(group = "ch.qos.logback", name = "logback-classic", version = "1.2.3")
    implementation(group = "ch.qos.logback", name = "logback-access", version = "1.2.3")
    implementation(group = "org.slf4j", name = "slf4j-api", version = "1.7.12")
    implementation(group = "org.slf4j", name = "jul-to-slf4j", version = "1.7.12")
    implementation(group = "org.slf4j", name = "jcl-over-slf4j", version = "1.7.12")
    implementation(group = "org.slf4j", name = "log4j-over-slf4j", version = "1.7.12")

    implementation(group = "com.google.guava", name = "guava", version = "30.0-jre")
    implementation(group = "org.apache.httpcomponents", name = "httpclient", version = "4.5.13")
    implementation(group = "org.springframework.boot", name = "spring-boot-starter-webflux", version = "2.3.1.RELEASE")

    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter", version = "5.4.2")
}
