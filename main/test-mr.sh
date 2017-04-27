#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
#export GOPATH="$here/.."
echo $GOPATH
echo ""
echo "==> Part I"
go test -run Sequential github.com/6.824/mapreduce/...
echo ""
echo "==> Part II"
(cd "$here" && sh ./test-wc.sh > /dev/null)
echo ""
echo "==> Part III"
go test -run TestBasic github.com/6.824/mapreduce/...
echo ""
echo "==> Part IV"
go test -run Failure github.com/6.824/mapreduce/...
echo ""
echo "==> Part V (challenge)"
(cd "$here" && sh ./test-ii.sh > /dev/null)

rm "$here"/mrtmp.* "$here"/diff.out
