#!/bin/bash

SETUP_DIR=/Users/hiranya/Projects/mdcc/sandbox/setup
mvn clean install -o

cp -v core/target/*.jar $SETUP_DIR/node1/lib
cp -v core/target/*.jar $SETUP_DIR/node2/lib
cp -v core/target/*.jar $SETUP_DIR/node3/lib
cp -v core/target/*.jar $SETUP_DIR/node4/lib
cp -v core/target/*.jar $SETUP_DIR/node5/lib

