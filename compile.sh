#!/bin/bash

cd `dirname $0`

TARGETDIR=temp_build_dir

mkdir $TARGETDIR &>/dev/null

CLASSPATH=.:../

for jar in `pwd`/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

export CLASSPATH

cd src

find . -name \*.java | xargs javac -O -g -d ../$TARGETDIR || exit 1

cd ../$TARGETDIR

cp ../trusted_authorities.jks .

mkdir -p config

cp ../config/config.properties ../config/monitoring.properties config/

jar cf ../alien.jar *

rm -f ../alien-users.jar ../alien-cs.jar

if [ "x$1" == "xall" ]; then
    echo "Preparing alien-users.jar"

    for dependency in ../lib/{FarmMonitor.jar,apmon.jar,bcp*.jar,catalina.jar,javax.json-api-*.jar,jline-*.jar,jopt-simple-*.jar,json-simple-*.jar,lazyj.jar,servlet-api.jar,tomcat-*.jar,ca-api*.jar,java-ca-lib*.jar,annotations-api.jar}; do
	jar -xf $dependency
    done

    rm -rf META-INF

    jar cf ../alien-users.jar *

    ## Now all the dependencies in a single file, for central services (+DB drivers, CA, everything else)
    echo "Preparing alien-cs.jar"

    for dependency in ../lib/*.jar; do
        jar -xf $dependency
    done

    rm -rf META-INF

    jar cf ../alien-cs.jar *
else
    echo "Have only packaged alien.jar, if you need the other ones just run './compile.sh all'"
fi

## Cleanup

cd ..

rm -rf $TARGETDIR
