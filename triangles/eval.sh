#!/usr/bin/env bash

INPUT=$1
ALGO=$2
ssizes=(5000 10000 20000 30000 40000)
cd src || exit

for ssize in "${ssizes[@]}"; do 
    for i in {1..20}; do 
        java Main -"$ALGO" "$ssize" ../input/"$INPUT" > ../output/"$INPUT"_"$ALGO"_"$ssize"_"$i".out
    done
done