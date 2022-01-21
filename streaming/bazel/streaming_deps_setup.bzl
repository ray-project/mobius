load("@com_github_ray_streaming//java:dependencies.bzl", "gen_streaming_java_deps")

def streaming_deps_setup():
    gen_streaming_java_deps()