#!/bin/bash

cd `dirname $0`

mkdir build_eclipse &>/dev/null

CLASSPATH=.:../

for jar in `pwd`/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

export CLASSPATH

cd src

find . -name \*.java | xargs javac -O -g -d ../build_eclipse || exit 1

cd ../build_eclipse

cp ../trusted_authorities.jks .

mkdir -p config

cp ../config/config.properties ../config/monitoring.properties config/

jar cf ../alien.jar *

