#!/bin/bash

CLASSPATH=bin:build_eclipse

for jar in lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

CLASSPATH="$CLASSPATH:alien.jar"

export CLASSPATH

java -Duserid=$(id -u) -DAliEnConfig=$HOME/.alien/testVO/config "$@" $(pwd) 

