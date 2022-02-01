#!/usr/bin/env bash

trap 'exit 1' INT

echo "Compiling raft.go..."
go test -c -race
chmod +x ./raft.test
echo "Running test $1 for $2 iter..."
for i in $(seq 1 "$2"); do
    echo -ne "\r$i/$2 "
    LOG="$1_$i.log"

    if time ./raft.test -test.run "$1" &> "$LOG"; then
        echo "Success"
    else
        echo "Failed - saving log at FAILED_$LOG"
        mv "$LOG" "FAILED_$LOG"
    fi
done
