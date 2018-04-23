# !/bin/bash

TIMES=100

for (( i=0; i < $TIMES; i++ )); do
  echo $i
  go test -run $@
  result=$?
  echo $result
  if [[ $result -ne 0 ]]; then
    break
  fi
done
