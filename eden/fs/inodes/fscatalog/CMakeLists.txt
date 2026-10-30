# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

include(generated_targets.cmake)

# `eden fsck` runs eden_fsck, which generated_targets.cmake leaves out with the
# other BUCK binaries. Like edenfs, it's installed in bin; a release has both in
# libexec/eden, where the CLI looks for it (cmd_util.get_fsck_command).
if(NOT WIN32)
  add_executable(eden_fsck eden_fsck.cpp)
  target_link_libraries(
    eden_fsck
    PRIVATE
      eden_fs_inodes_fscatalog_fsinodecatalog
      eden_fs_inodes_overlay_overlay_checker
      Folly::folly
  )
  install(TARGETS eden_fsck RUNTIME DESTINATION bin)
endif()

if(BUILD_TESTS)
  add_subdirectory(test)
endif()
