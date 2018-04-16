#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=mysort2GB.log
#SBATCH --wait-all-nodes=1 
z/usr/bin/time -f "Time taken to sort 2GB in (seconds): \t%e" java MySort 2GB 4 5000000
valsort /tmp/FinalSortedFile.txt	

