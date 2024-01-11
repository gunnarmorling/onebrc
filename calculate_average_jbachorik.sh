#!/bin/sh
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

source ~/.sdkman/bin/sdkman-init.sh > /dev/null
sdk use java 21.0.1-tem > /dev/null
JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC  -Xmx512m -XX:+UseInlineCaches -XX:CICompilerCount=4 -Xmx512m -XX:+UseInlineCaches -XX:MaxInlineSize=512 -XX:FreqInlineSize=800 -XX:InlineSmallCode=190 -XX:CompileThreshold=2"
time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_jbachorik $@
