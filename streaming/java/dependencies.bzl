load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

def gen_streaming_java_deps():
    maven_install(
        name = "ray_streaming_maven",
        artifacts = [
            "com.esotericsoftware:kryo:4.0.0",
            "com.esotericsoftware.minlog:minlog:1.2",
            "com.esotericsoftware.reflectasm:reflectasm:1.07",
            "com.google.guava:guava:27.0.1-jre",
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.8.5",
            "com.github.davidmoten:flatbuffers-java:1.9.0.1",
            "com.typesafe:config:1.3.2",
            "de.javakaffee:kryo-serializers:0.42",
            "org.apache.commons:commons-lang3:3.4",
            "org.aeonbits.owner:owner:1.0.10",
            "org.mockito:mockito-all:1.10.19",
            "org.apache.commons:commons-lang3:3.3.2",
            "org.mockito:mockito-all:1.10.19",
            "org.powermock:powermock-module-testng:1.6.6",
            "org.powermock:powermock-api-mockito:1.6.6",
            "commons-collections:commons-collections:3.2.2",
            "joda-time:joda-time:2.10.14",
            "commons-io:commons-io:2.5",
            "net.java.dev.jna:jna:5.5.0",
            maven.artifact(
                group = "io.ray",
                artifact = "ray-api",
                version = "2.11.0",
                neverlink = True
            ),
            maven.artifact(
                group = "io.ray",
                artifact = "ray-runtime",
                version = "2.11.0",
                neverlink = True
            ),
            maven.artifact(
                group = "org.apache.arrow",
                artifact = "arrow-vector",
                version = "5.0.0",
                exclusions = [
                    "io.netty:netty-buffer",
                    "io.netty:netty-common",
                ]
            ),
            maven.artifact(
                group = "de.ruedigermoeller",
                artifact = "fst",
                version = "2.56",
                exclusions = [
                    "com.fasterxml.jackson.core:jackson-core",
                ]
            ),
        ],
        repositories = [
            "https://repo1.maven.org/maven2/",
        ],
    )
