1. Unzip the folder and look for the “src” folder into it.
2. Navigate to src folder and to compile the .java files, we can compile them using
	javac *.java
3. Or another way to compile them is using Makefile with the following command on cluster.
		make
4.Once the .java files are compiled and .class files are created, use the slurm command to excecute the MySsort code
5.In slurm file split command is included to split the file before the program starts exceuting
5. (a) sbatch mySort2GB.slurm (By deafult 2GB file is sorted by using 4 threads each on chunks of Size 500MB. Similary different slurm are created to execute 2 and 8 threads on each chunks of size 5000MB)
	(b) sbatch mySort2oGB.slurm (By deafult 20GB file is sorted by using 4 threads each on chunks of Size 2GB. )
6. To verify the correctness of the output file produced, run below valsort command:
	valsort /tmp/FinalSortedOutput.txt
7. Refer the mySort2GBlog and mySort20GBlog to see the corresspoding execution of the sorting process. (valsort to verify the output of the sorted file is already included in the slurm file)
8. Run linsort2GB.slurm and linsort20GB.slurm and refer linsort2B.log and linsort20GB.log to see the corresponding execution of Linsort logs.

