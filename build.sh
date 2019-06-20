#!/bin/bash

export JAVA_HOME=$BUILD_KIT_PATH/java/jdk-1.8-8u20/
export PATH=${JAVA_HOME}/bin:${PATH}
export GRADLE_USER_HOME=~/.m2

bash ./gradlew
