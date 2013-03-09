#!/bin/sh

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set MDCC_HOME if not already set
[ -z "$MDCC_HOME" ] && MDCC_HOME=`cd "$PRGDIR/.." ; pwd`
MDCC_CLASSPATH="$MDCC_HOME/lib"
for f in $MDCC_HOME/lib/*.jar
do
    MDCC_CLASSPATH=$MDCC_CLASSPATH:$f
done

MDCC_CLASSPATH=$MDCC_CLASSPATH:$MDCC_HOME/lib

java -Duser.dir=$MDCC_HOME -Dmdcc.config.dir=$MDCC_HOME/conf -classpath $MDCC_CLASSPATH edu.ucsb.cs.mdcc.paxos.AppServer $*