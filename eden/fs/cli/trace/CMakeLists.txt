# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

# `eden trace` runs this as eden_trace_stream. Like edenfs, it's installed in
# bin; a release has both in libexec/eden, where the CLI looks for it
# (trace_cmd.get_trace_stream_command).
add_executable(eden_trace_stream trace_stream.cpp)
target_link_libraries(
  eden_trace_stream
  PRIVATE
    streamingeden_thrift_cpp
    eden_fs_config_toml_file_util
    edencommon::edencommon_utils
    FBThrift::thriftcpp2
    Folly::folly
    fmt::fmt
    cpptoml
)
install(TARGETS eden_trace_stream RUNTIME DESTINATION bin)
