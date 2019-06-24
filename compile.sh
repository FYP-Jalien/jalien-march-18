#!/bin/bash

CURDIR="$(pwd)"
ARG="${1}"

# detect REAL location of execution even if we are a symlink and/or in a symlinked dir
if [[ -z "${JALIEN_HOME}" ]]; then
  ## find the location of jalien script
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "${SOURCE}" ]; do ## resolve $SOURCE until the file is no longer a symlink
    JALIEN_HOME="$( cd -P "$(dirname "${SOURCE}" )" && pwd )" ##"
    SOURCE="$(readlink "${SOURCE}")" ##"
    [[ "${SOURCE}" != /* ]] && SOURCE="${JALIEN_HOME}/${SOURCE}" ## if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  JALIEN_HOME="$(cd -P "$( dirname "${SOURCE}" )" && pwd)" ##"
  export JALIEN_HOME
fi

cd "${JALIEN_HOME}"

BUILDDIR="${JALIEN_HOME}/temp_build_dir"
mkdir -p ${BUILDDIR}/config &>/dev/null

CLASSPATH=.:../

JAR_LIST_LIB="";
for libjar in ${JALIEN_HOME}/lib/*.jar; do JAR_LIST_LIB="${JAR_LIST_LIB}${libjar}:" ; done;
JAR_LIST_LIB=$( echo "${JAR_LIST_LIB}" | sed 's/.$//')

export CLASSPATH="${CLASSPATH}:${JAR_LIST_LIB}"

# prepare files in build dir
cp ${JALIEN_HOME}/trusted_authorities.jks ${BUILDDIR}/
cp ${JALIEN_HOME}/config/config.properties ${JALIEN_HOME}/config/monitoring.properties ${BUILDDIR}/config/

# always generate alien.jar
find ${JALIEN_HOME}/src/main -name "*.java" | xargs javac -Xlint:-options -source 8 -target 8 -O -g -d ${BUILDDIR} || { echo "javac of src/*.java failed" ; exit 1; }

# Clean up all previous jar generated files
rm -rf ${JALIEN_HOME}/alien.jar

# create general jar file
cd ${BUILDDIR} && jar cf ${JALIEN_HOME}/alien.jar *

generate_users () {
  echo "Preparing alien-users.jar"
  cd "${BUILDDIR}"
  # extract all specified java classes
  for dependency in ${JALIEN_HOME}/lib/{FarmMonitor.jar,apmon.jar,bcp*.jar,catalina.jar,javax.json-api-*.jar,jline-*.jar,jopt-simple-*.jar,json-simple-*.jar,lazyj.jar,servlet-api.jar,tomcat-*.jar,ca-api*.jar,java-ca-lib*.jar,annotations-api.jar,jaspic-api.jar}; do
    jar xf ${dependency}
  done
  rm -rf META-INF

  # clean up of previous jar
  rm -f ${JALIEN_HOME}/alien-users.jar

  # create common jar files with the specified dependencies
  jar cf ${JALIEN_HOME}/alien-users.jar *
}

generate_cs () {
  ## Now all the dependencies in a single file, for central services (+DB drivers, CA, everything else)
  echo "Preparing alien-cs.jar"
  cd "${BUILDDIR}"
  # extract all specified java classes
  for dependency in ${JALIEN_HOME}/lib/*.jar; do
    jar xf ${dependency}
  done
  rm -rf META-INF

  # clean up of previous jar
  rm -f ${JALIEN_HOME}/alien-cs.jar

  # create common jar files with the specified dependencies
  jar cf ${JALIEN_HOME}/alien-cs.jar *
}


if [[ "${ARG}" == "all" ]]; then
  generate_users
  generate_cs
elif [[ "${ARG}" == "cs" ]]; then
  generate_cs
elif [[ "${ARG}" == "users" ]]; then
  generate_users
fi

## Cleanup
[[ -e "${BUILDDIR}" ]] && rm -rf ${BUILDDIR}
cd "${CURDIR}"
