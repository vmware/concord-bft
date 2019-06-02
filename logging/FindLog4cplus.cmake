
#--------------------------------------------------------------------------------
# Copyright (c) 2012-2013, Lars Baehren <lbaehren@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#  * Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#--------------------------------------------------------------------------------

# - Check for the presence of LOG4CPLUS
#
# The following variables are set when LOG4CPLUS is found:
#  LOG4CPLUS_FOUND      = Set to true, if all components of LOG4CPLUS have been found.
#  LOG4CPLUS_INCLUDES   = Include path for the header files of LOG4CPLUS
#  LOG4CPLUS_LIBRARIES  = Link these to use LOG4CPLUS

if (NOT LOG4CPLUS_FOUND)

  if (NOT LOG4CPLUS_ROOT_DIR)
    set (LOG4CPLUS_ROOT_DIR ${CMAKE_INSTALL_PREFIX})
  endif (NOT LOG4CPLUS_ROOT_DIR)

  ##_____________________________________________________________________________
  ## Check for the header files

  find_path (LOG4CPLUS_INCLUDES
    NAMES log4cplus/config.hxx log4cplus/appender.h log4cplus/loglevel.h
    HINTS ${LOG4CPLUS_ROOT_DIR} ${CMAKE_INSTALL_PREFIX}
    PATH_SUFFIXES include
    )

  ##_____________________________________________________________________________
  ## Check for the library

  find_library (LOG4CPLUS_LIBRARIES log4cplus
    HINTS ${LOG4CPLUS_ROOT_DIR} ${CMAKE_INSTALL_PREFIX}
    PATH_SUFFIXES lib
    )

  ##_____________________________________________________________________________
  ## Actions taken when all components have been found

  find_package_handle_standard_args (LOG4CPLUS DEFAULT_MSG LOG4CPLUS_LIBRARIES LOG4CPLUS_INCLUDES)

  if (LOG4CPLUS_FOUND)
    if (NOT LOG4CPLUS_FIND_QUIETLY)
      message (STATUS "Found components for log4cplus")
      message (STATUS "LOG4CPLUS_ROOT_DIR  = ${LOG4CPLUS_ROOT_DIR}")
      message (STATUS "LOG4CPLUS_INCLUDES  = ${LOG4CPLUS_INCLUDES}")
      message (STATUS "LOG4CPLUS_LIBRARIES = ${LOG4CPLUS_LIBRARIES}")
    endif (NOT LOG4CPLUS_FIND_QUIETLY)
  else (LOG4CPLUS_FOUND)
    if (LOG4CPLUS_FIND_REQUIRED)
      message (FATAL_ERROR "Could not find log4cplus!")
    endif (LOG4CPLUS_FIND_REQUIRED)
  endif (LOG4CPLUS_FOUND)

  ##_____________________________________________________________________________
  ## Mark advanced variables

  mark_as_advanced (
    LOG4CPLUS_ROOT_DIR
    LOG4CPLUS_INCLUDES
    LOG4CPLUS_LIBRARIES
    )

endif (NOT LOG4CPLUS_FOUND)
