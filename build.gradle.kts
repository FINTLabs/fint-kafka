import org.springframework.boot.gradle.plugin.SpringBootPlugin

buildscript {
    repositories {
        gradlePluginPortal()
    }
    dependencies {
        classpath(platform("com.fasterxml.jackson:jackson-bom:2.22.2"))
        constraints {
            classpath("org.apache.httpcomponents.client5:httpclient5:5.6.4")
            classpath("org.apache.httpcomponents.core5:httpcore5:5.4.3")
            classpath("org.apache.httpcomponents.core5:httpcore5-h2:5.4.3")
            classpath("org.apache.commons:commons-lang3:3.18.0")
        }
    }
}

plugins {
    id("org.springframework.boot") version "3.5.16" apply false
    id("io.spring.dependency-management") version "1.1.7"
    id("java-library")
    id("maven-publish")
    id("java-test-fixtures")
    id("io.github.ben-manes.versions") version "0.61.0"
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.spring") version "2.4.10"
    kotlin("plugin.lombok") version "2.4.10"
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0"
}

group = "no.novari"
version = findProperty("version")?.toString() ?: "1.0-SNAPSHOT"

extra["kotlin.version"] = "2.4.10"
extra["jackson-bom.version"] = "2.21.5"
extra["log4j2.version"] = "2.25.5"
extra["netty.version"] = "4.1.137.Final"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
    withSourcesJar()
}

kotlin {
    jvmToolchain(25)
}

ktlint {
    version.set("1.8.0")
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.fintlabs.no/releases")
    }
    mavenCentral()
}

dependencyManagement {
    imports {
        mavenBom(SpringBootPlugin.BOM_COORDINATES)
    }
}

dependencies {
    constraints {
        implementation("at.yawk.lz4:lz4-java:1.11.2") {
            because("Fixes CVE-2026-59949 in the kafka-clients transitive dependency")
        }
    }

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-json")

    compileOnly("org.springframework.boot:spring-boot-starter-actuator")

    implementation("org.jetbrains.kotlin:kotlin-reflect")

    api("org.springframework.kafka:spring-kafka")

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    annotationProcessor("org.projectlombok:lombok")

    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
    testAnnotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-actuator")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.mockito.kotlin:mockito-kotlin:6.3.0")

    testFixturesCompileOnly("org.projectlombok:lombok")
    testFixturesAnnotationProcessor("org.projectlombok:lombok")
    testFixturesAnnotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    testFixturesImplementation("org.junit.jupiter:junit-jupiter")
    testFixturesImplementation("org.assertj:assertj-core")
    testFixturesImplementation("org.mockito:mockito-core")
    testFixturesImplementation("com.fasterxml.jackson.core:jackson-databind")
    testFixturesImplementation("org.apache.logging.log4j:log4j-api")
    testFixturesApi("org.springframework.kafka:spring-kafka")
    testFixturesApi("org.springframework.kafka:spring-kafka-test")
}

tasks.test {
    useJUnitPlatform()
}

tasks.named("check") {
    dependsOn("ktlintCheck")
}

publishing {
    repositories {
        maven {
            url = uri("https://repo.fintlabs.no/releases")
            credentials {
                username = System.getenv("REPOSILITE_USERNAME")
                password = System.getenv("REPOSILITE_PASSWORD")
            }
            authentication {
                create<BasicAuthentication>("basic")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
