#!/bin/bash

ITERATIONS=1

for (( i=1; i<=ITERATIONS; i++ ))
do
    echo "Iteration $i"

    python3 ../scripts/tcGen.py > ../tests/test_$i.in
    
    echo "Test cases for iteration $i generated"
    ./grader ../engine < ../tests/test_$i.in > "../scripts/testOutput/output_$i.txt" 2>>"../scripts/testOutput/error.txt"

    echo "Iteration $i completed"
done