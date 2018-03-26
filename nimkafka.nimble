# Package

version       = "0.1.0"
author        = "Spencer Stirling"
description   = "Kafka client for Nim"
license       = "MIT"
srcDir        = "src"
skipFiles     = @["example_low_level.nim"]

# Deps

requires: "nim >= 0.18.0"

import distros
foreignDep "librdkafka"
