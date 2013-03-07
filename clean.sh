#!/bin/bash

SETUP_DIR=/Users/hiranya/Projects/mdcc/sandbox/setup

rm -rfv $SETUP_DIR/node1/db/hbase
mkdir -p $SETUP_DIR/node1/db/hbase/data
mkdir -p $SETUP_DIR/node1/db/hbase/zk

rm -rfv $SETUP_DIR/node2/db/hbase
mkdir -p $SETUP_DIR/node2/db/hbase/data
mkdir -p $SETUP_DIR/node2/db/hbase/zk

rm -rfv $SETUP_DIR/node3/db/hbase
mkdir -p $SETUP_DIR/node3/db/hbase/data
mkdir -p $SETUP_DIR/node3/db/hbase/zk

rm -rfv $SETUP_DIR/node4/db/hbase
mkdir -p $SETUP_DIR/node4/db/hbase/data
mkdir -p $SETUP_DIR/node4/db/hbase/zk

rm -rfv $SETUP_DIR/node5/db/hbase
mkdir -p $SETUP_DIR/node5/db/hbase/data
mkdir -p $SETUP_DIR/node5/db/hbase/zk

