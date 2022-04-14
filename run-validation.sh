#!/bin/bash
if [ -z "$1" ];
then
  echo "Give me a number!!";
  echo "Usage:";
  echo "    ./run-full-circle-of-validation.sh <number>"
  exit;
fi
n=2;
if [ -n "$2" ];
then
  n=$2
fi

echo "./gen_RMAT -s $1;"
./gen_RMAT -s $1;
echo "./gen_valid_info -in rmat-$1;"
./gen_valid_info -in rmat-$1;
echo "";
echo "mpiexec -n $n python -m mpi4py mst.py rmat-$1;"
mpiexec -n $n python -m mpi4py mst.py rmat-$1;
echo "";
echo 'Validation:';
echo "./validation -in_graph rmat-$1 -in_result rmat-$1.mst -in_valid rmat-$1.vinfo"
./validation -in_graph rmat-$1 -in_result rmat-$1.mst -in_valid rmat-$1.vinfo
