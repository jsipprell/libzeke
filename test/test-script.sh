#!/bin/bash


set +x
set -e

abort_script() {
  set +x
  set +e
}

trap abort_script EXIT
declare -i e=0
while true; do
  set +e
  seq 1 5 | \
    { while read x; do
        sleep 1
        echo $x
      done
    }
  let e=$?
  [[ $e -ne 0 ]] && exit $e
  exit 42
done

