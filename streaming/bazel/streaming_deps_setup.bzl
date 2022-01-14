load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@com_github_ray_project_ray//bazel:ray_deps_setup.bzl", "auto_http_archive")

def streaming_deps_setup():
    auto_http_archive(
        name = "rules_proto_grpc",
        url = "https://github.com/rules-proto-grpc/rules_proto_grpc/archive/a74fef39c5fe636580083545f76d1eab74f6450d.tar.gz",
        sha256 = "2f6606151ec042e23396f07de9e7dcf6ca9a5db1d2b09f0cc93a7fc7f4008d1b",
    )

    auto_http_archive(
        name = "com_github_google_flatbuffers",
        url = "https://github.com/google/flatbuffers/archive/63d51afd1196336a7d1f56a988091ef05deb1c62.tar.gz",
        sha256 = "3f469032571d324eabea88d7014c05fec8565a5877dbe49b2a52d8d1a0f18e63",
    )

    auto_http_archive(
        name = "bazel_common",
        url = "https://github.com/google/bazel-common/archive/084aadd3b854cad5d5e754a7e7d958ac531e6801.tar.gz",
        sha256 = "a6e372118bc961b182a3a86344c0385b6b509882929c6b12dc03bb5084c775d5",
    )

    auto_http_archive(
        name = "bazel_skylib",
        strip_prefix = None,
        url = "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
        sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
    )

    http_archive(
        name = "com_google_protobuf",
        strip_prefix = "protobuf-3.16.0",
        urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.16.0.tar.gz"],
        sha256 = "7892a35d979304a404400a101c46ce90e85ec9e2a766a86041bb361f626247f5",
    )

    http_archive(
        name = "rules_python",
        sha256 = "a30abdfc7126d497a7698c29c46ea9901c6392d6ed315171a6df5ce433aa4502",
        strip_prefix = "rules_python-0.6.0",
        url = "https://github.com/bazelbuild/rules_python/archive/0.6.0.tar.gz",
    )

    auto_http_archive(
        name = "com_github_grpc_grpc",
        # NOTE: If you update this, also update @boringssl's hash.
        url = "https://github.com/grpc/grpc/archive/refs/tags/v1.38.1.tar.gz",
        sha256 = "f60e5b112913bf776a22c16a3053cc02cf55e60bf27a959fd54d7aaf8e2da6e8",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:grpc-cython-copts.patch",
            "@com_github_ray_project_ray//thirdparty/patches:grpc-python.patch",
            "@com_github_ray_project_ray//thirdparty/patches:grpc-windows-python-header-path.patch",
        ],
    )

    auto_http_archive(
        # This rule is used by @com_github_nelhage_rules_boost and
        # declaring it here allows us to avoid patching the latter.
        name = "boost",
        build_file = "@com_github_nelhage_rules_boost//:BUILD.boost",
        sha256 = "83bfc1507731a0906e387fc28b7ef5417d591429e51e788417fe9ff025e116b1",
        url = "https://boostorg.jfrog.io/artifactory/main/release/1.74.0/source/boost_1_74_0.tar.bz2",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:boost-exception-no_warn_typeid_evaluated.patch",
        ],
    )

    auto_http_archive(
        name = "com_google_googletest",
        url = "https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz",
        sha256 = "b4870bf121ff7795ba20d20bcdd8627b8e088f2d1dab299a031c1034eddc93d5",
    )

    #http_archive(
    #    name = "zlib",
    #    build_file = "@com_google_protobuf//:third_party/zlib.BUILD",
    #    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    #    strip_prefix = "zlib-1.2.11",
    #    urls = [
    #        "https://mirror.bazel.build/zlib.net/zlib-1.2.11.tar.gz",
    #        "https://zlib.net/zlib-1.2.11.tar.gz",
    #    ],
    #)

    http_archive(
        name = "nlohmann_json",
        strip_prefix = "json-3.9.1",
        urls = ["https://github.com/nlohmann/json/archive/v3.9.1.tar.gz"],
        sha256 = "4cf0df69731494668bdd6460ed8cb269b68de9c19ad8c27abc24cd72605b2d5b",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.nlohmann_json",
    )

    auto_http_archive(
        name = "io_opencensus_cpp",
        url = "https://github.com/census-instrumentation/opencensus-cpp/archive/b14a5c0dcc2da8a7fc438fab637845c73438b703.zip",
        sha256 = "6592e07672e7f7980687f6c1abda81974d8d379e273fea3b54b6c4d855489b9d",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:opencensus-cpp-harvest-interval.patch",
            "@com_github_ray_project_ray//thirdparty/patches:opencensus-cpp-shutdown-api.patch",
        ],
    )


    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    auto_http_archive(
        name = "com_google_absl",
        url = "https://github.com/abseil/abseil-cpp/archive/refs/tags/20210324.2.tar.gz",
        sha256 = "59b862f50e710277f8ede96f083a5bb8d7c9595376146838b9580be90374ee1f",
    )

    auto_http_archive(
        name = "com_github_spdlog",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.spdlog",
        urls = ["https://github.com/gabime/spdlog/archive/v1.7.0.zip"],
        sha256 = "c8f1e1103e0b148eb8832275d8e68036f2fdd3975a1199af0e844908c56f6ea5",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    auto_http_archive(
        name = "com_github_jupp0r_prometheus_cpp",
        url = "https://github.com/jupp0r/prometheus-cpp/archive/60eaa4ea47b16751a8e8740b05fe70914c68a480.tar.gz",
        sha256 = "ec825b802487ac18b0d98e2e8b7961487b12562f8f82e424521d0a891d9e1373",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:prometheus-windows-headers.patch",
            # https://github.com/jupp0r/prometheus-cpp/pull/225
            "@com_github_ray_project_ray//thirdparty/patches:prometheus-windows-zlib.patch",
            "@com_github_ray_project_ray//thirdparty/patches:prometheus-windows-pollfd.patch",
        ],
    )

    auto_http_archive(
        name = "msgpack",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.msgpack",
        url = "https://github.com/msgpack/msgpack-c/archive/8085ab8721090a447cf98bb802d1406ad7afe420.tar.gz",
        sha256 = "83c37c9ad926bbee68d564d9f53c6cbb057c1f755c264043ddd87d89e36d15bb",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:msgpack-windows-iovec.patch",
        ],
    )

    auto_http_archive(
        # This rule is used by @com_github_nelhage_rules_boost and
        # declaring it here allows us to avoid patching the latter.
        name = "boost",
        build_file = "@com_github_nelhage_rules_boost//:BUILD.boost",
        sha256 = "83bfc1507731a0906e387fc28b7ef5417d591429e51e788417fe9ff025e116b1",
        url = "https://boostorg.jfrog.io/artifactory/main/release/1.74.0/source/boost_1_74_0.tar.bz2",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:boost-exception-no_warn_typeid_evaluated.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_nelhage_rules_boost",
        # If you update the Boost version, remember to update the 'boost' rule.
        url = "https://github.com/nelhage/rules_boost/archive/652b21e35e4eeed5579e696da0facbe8dba52b1f.tar.gz",
        sha256 = "c1b8b2adc3b4201683cf94dda7eef3fc0f4f4c0ea5caa3ed3feffe07e1fb5b15",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:rules_boost-windows-linkopts.patch",
        ],
    )

    auto_http_archive(
        name = "rules_jvm_external",
        url = "https://github.com/bazelbuild/rules_jvm_external/archive/2.10.tar.gz",
        sha256 = "5c1b22eab26807d5286ada7392d796cbc8425d3ef9a57d114b79c5f8ef8aca7c",
    )

    http_archive(
        name = "io_opencensus_proto",
        strip_prefix = "opencensus-proto-0.3.0/src",
        urls = ["https://github.com/census-instrumentation/opencensus-proto/archive/v0.3.0.tar.gz"],
        sha256 = "b7e13f0b4259e80c3070b583c2f39e53153085a6918718b1c710caf7037572b0",
    )

    auto_http_archive(
        name = "com_github_redis_hiredis",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.hiredis",
        url = "https://github.com/redis/hiredis/archive/392de5d7f97353485df1237872cb682842e8d83f.tar.gz",
        sha256 = "2101650d39a8f13293f263e9da242d2c6dee0cda08d343b2939ffe3d95cf3b8b",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:hiredis-windows-msvc.patch",
        ],
    )