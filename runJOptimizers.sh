#!/bin/bash

cd `dirname $0`

pkill -f alien.JOptimizers

if [ -z "$JALIEN_HOME" ]; then
    JALIEN_HOME=`pwd`
fi

CONFIG_DIR=${JALIEN_CONFIG_DIR:-${HOME}/.alien/config}

# This flag helps debugging SSL connection errors
#    -Djavax.net.debug=all \

./run.sh \
    -XX:CompileThreshold=5 \
    -DAliEnConfig=${CONFIG_DIR} \
    -Dsun.security.ssl.allowUnsafeRenegotiation=true \
    -Djavax.net.debug=all \
    alien.JOptimizers \
&>joptimizers.log </dev/null &
