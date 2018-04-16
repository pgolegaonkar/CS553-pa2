#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=linsort2GB.log
#SBATCH --wait-all-nodes=1 
LC_ALL=C /usr/bin/time -f "Time taken to Sort 2GB of data using LinSort  in seconds : \t%e" sort -o /tmp/data-2GB /input/data-2GB.in 
valsort /tmp/data-2GB

