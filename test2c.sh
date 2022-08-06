#!/bin/bash
int1=1
# while(($int1<=100))
# do
echo $int1
#	$int1==$int1+1
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
/usr/local/go/bin/go test -timeout 10m -run ^^TestOneSnapshot2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
echo $int1
/usr/local/go/bin/go test -timeout 10m -run ^TestSnapshotRecover2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
echo $int1
/usr/local/go/bin/go test -timeout 10m -run ^TestSnapshotRecoverManyClients2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
echo $int1
/usr/local/go/bin/go test -timeout 10m -run ^TestSnapshotUnreliable2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
echo $int1
/usr/local/go/bin/go test -timeout 10m -run ^TestSnapshotUnreliableRecover2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
echo $int1
/usr/local/go/bin/go test -timeout 10m -run ^TestSnapshotUnreliableRecoverConcurrentPartition2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
rm -rf ./kv/test_raftstore/tmp/test-raftstore*
echo "finish"
# done
