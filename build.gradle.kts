buildscript {
    repositories {
        gradlePluginPortal()
    }
    dependencies {
        classpath(platform(libs.jackson2.bom.buildscript))
        constraints {
            classpath(libs.httpclient5)
            classpath(libs.httpcore5)
            classpath(libs.httpcore5.h2)
            classpath(libs.commons.lang3)
        }
    }
}

plugins {
    id("java-library")
    id("maven-publish")
    id("java-test-fixtures")
    alias(libs.plugins.ben.manes.versions)
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.kotlin.lombok)
    alias(libs.plugins.ktlint)
}

group = "no.novari"
version = findProperty("version")?.toString() ?: "1.0-SNAPSHOT"

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
    version.set(libs.versions.ktlint.cli)
}

// ktlint embeds the Kotlin 2.2 compiler and breaks when the Kotlin Gradle plugin aligns it to a newer version.
// Its bundled logback is outdated, so it is lifted to the version Spring Boot used to provide.
afterEvaluate {
    val ktlintKotlinVersion =
        libs.versions.ktlint.kotlin
            .get()
    val logbackVersion = libs.versions.logback.get()
    configurations
        .matching { it.name.startsWith("ktlint") }
        .configureEach {
            resolutionStrategy.eachDependency {
                if (requested.group == "org.jetbrains.kotlin") {
                    useVersion(ktlintKotlinVersion)
                }
                if (requested.group == "ch.qos.logback") {
                    useVersion(logbackVersion)
                }
            }
        }
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.fintlabs.no/releases")
    }
    mavenCentral()
}

val bomConfigurations =
    listOf(
        "implementation",
        "testFixturesImplementation",
        "annotationProcessor",
        "testAnnotationProcessor",
        "testFixturesAnnotationProcessor",
    )

dependencies {
    bomConfigurations.forEach { configuration ->
        add(configuration, platform(libs.jackson2.bom))
        add(configuration, platform(libs.log4j.bom))
        add(configuration, platform(libs.netty.bom))
        add(configuration, platform(libs.spring.boot.dependencies))
    }

    constraints {
        implementation(libs.lz4.java) {
            because("Fixes known vulnerabilities in the kafka-clients transitive dependency")
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
    testImplementation(libs.mockito.kotlin)

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
