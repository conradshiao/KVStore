#!/bin/bash                                                                                                                                              
if [ ! "$1" ]; then
  echo "Error: Must provide the name of the test to run!"
  exit
fi

STR="build/tests/userprog/"
STR="$STR$1"
STR="$STR.result"
make $STR
