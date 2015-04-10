#!/bin/bash
if [ ! "$1" ]; then
  echo "Error: Must provide the number of checkpoint to run!"
  exit
fi

STR="ch"
STR="$STR$1"
make clean
./prepare $STR
make check
