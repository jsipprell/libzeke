#!/bin/bash

shopt -s nullglob
d="$(dirname $0)"
s="$(basename $0)"
plat="$(uname -s | tr A-Z a-z)"
declare -a suppfiles=(`ls "${d}/valgrind-suppression"* "${d}/valgrind-$plat-suppression"*`)

shopt -u nullglob
declare -a supps=()
for s in "${suppfiles[@]}"; do
  supps+=("--suppressions=$s")
done

exec valgrind --tool=memcheck \
         --dsymutil=yes \
         --leak-check=full \
         --track-origins=yes \
         --show-leak-kinds=definite \
         "${supps[@]}" \
         "$@"
