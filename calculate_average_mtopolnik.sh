#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.1-open 1>&2
# -XX:+UnlockDiagnosticVMOptions -XX:PrintAssemblyOptions=intel -XX:CompileCommand=print,*.CalculateAverage_mtopolnik::recordMeasurementAndAdvanceCursor"
time java -Xmx256m --enable-preview --add-modules=jdk.incubator.vector \
  --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_mtopolnik
