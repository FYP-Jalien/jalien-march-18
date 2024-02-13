#!/bin/bash

cd `dirname $0`

echo "Killing old JCentral process"
pkill -f alien.JCentral

for ((i=0; i<10; i++)); do
    if pgrep -f alien.JCentral; then
        echo -n "."
        sleep 1
    else
        break
    fi
done

echo ""

if [ -z "$JALIEN_HOME" ]; then
    JALIEN_HOME=`pwd`
fi

CONFIG_DIR=${JALIEN_CONFIG_DIR:-${HOME}/.alien/config}

# This flag helps debugging SSL connection errors
#    -Djavax.net.debug=all \

export JAVA_HOME=${JAVA_HOME:-/opt/java}

export PATH=$JAVA_HOME/bin:$PATH

echo "Starting new JCentral process with `which java`"

JALIEN_MEM="-Xms2g -Xmx16g" JALIEN_GC="-XX:+UseZGC" nohup ./run.sh \
    -XX:CompileThreshold=5 \
    -DAliEnConfig=${CONFIG_DIR} \
    -Dsun.security.ssl.allowUnsafeRenegotiation=true \
    alien.JCentral \
&>jcentral.log </dev/null &

disown

echo "Done"
