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
# sdk use java 21.0.1-graal 1>&2


JAVA_OPTS="-XX:+UseStringDeduplication -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC"
time java --enable-preview  -classpath /Users/adria.cabezasantanna/personal_projects/1brc/target/classes dev.morling.onebrc.CalculateAverage_adriacabeza

