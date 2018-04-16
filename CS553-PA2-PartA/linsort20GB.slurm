#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=linsort20GB.log
#SBATCH --wait-all-nodes=1 
LC_ALL=C /usr/bin/time -f "Time taken to Sort 20GB of data using LinSort  in seconds : \t%e" sort -o /tmp/data-20GB /input/data-20GB.in 
valsort /tmp/data-20GB

