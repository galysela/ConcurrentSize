shouldRunMeasurements=T # change to F for generating graphs from existing results folder
multicoreServer=T # change to F for a personal computer

warmupRepeats=5 # number of warmup runs, meant to warm up the JVM, that will not be part of the measurements' calculated average
repeats=10 # number of measured runs on which the average throughput is calculated
runtime=5 # how many seconds each run lasts
defaultDSSize=1000000 # initial number of elements in the data structure

if [ "$multicoreServer" = "T" ]; then
  JVMmem=15G # memory size of the JVM in all measurements
  workloadThreadsListWithoutSizeThread="[1,4,8,16,32,64]" # numbers of workload threads in overhead measurements without size thread
  workloadThreadsListWithOneSizeThread="[1,3,7,15,31,63]" # numbers of workload threads in overhead measurements with one size thread
  sizeThreads="[1,4,8,16]" # numbers of size threads in scalability measurements
  workloadThreadsWithVariableSizeThreads=32 # number of workload threads in scalability measurements
  workloadThreadsWithOneSizeThread=31 # number of workload threads in varying data-structure size measurements
  DSsizes="[1000000,10000000,100000000]" # data-structure sizes in varying data-structure size measurements
else
  JVMmem=8G
  workloadThreadsListWithoutSizeThread="[1,3,6]"
  workloadThreadsListWithOneSizeThread="[1,3,6]"
  sizeThreads="[1,3,5]"
  workloadThreadsWithVariableSizeThreads=2
  workloadThreadsWithOneSizeThread=3
  DSsizes="[500000,1000000,2000000]"
fi

if [ "$shouldRunMeasurements" = "T" ]; then
  ./compile.sh

  echo
  date
  echo "START RUNNING MEASUREMENTS"
  echo

fi

# All measurement commands
declare -a measurements=(
# Runs an update-heavy workload without a concurrent size thread and produces the top right graph and bars in Figures 7, 8 and 9
"python3 measurements/python_scripts/run_java_experiments_overhead.py ${defaultDSSize} 30-20 \"${workloadThreadsListWithoutSizeThread}\" 0 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs an update-heavy workload with a concurrent size thread and produces the bottom right graph and bars in Figures 7, 8 and 9
"python3 measurements/python_scripts/run_java_experiments_overhead.py ${defaultDSSize} 30-20 \"${workloadThreadsListWithOneSizeThread}\" 1 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs a read-heavy workload without a concurrent size thread and produces the top left graph and bars in Figures 7, 8 and 9
"python3 measurements/python_scripts/run_java_experiments_overhead.py ${defaultDSSize} 3-2 \"${workloadThreadsListWithoutSizeThread}\" 0 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs a read-heavy workload with a concurrent size thread and produces the bottom left graph and bars in Figures 7, 8 and 9
"python3 measurements/python_scripts/run_java_experiments_overhead.py ${defaultDSSize} 3-2 \"${workloadThreadsListWithOneSizeThread}\" 1 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs an update-heavy workload with different data structure sizes and produces the right graph in Figures 10 and 11
"python3 measurements/python_scripts/run_java_experiments_per_size.py \"${DSsizes}\" 30-20 ${workloadThreadsWithOneSizeThread} 1 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs a read-heavy workload with different data structure sizes and produces the left graph in Figures 10 and 11
"python3 measurements/python_scripts/run_java_experiments_per_size.py \"${DSsizes}\" 3-2 ${workloadThreadsWithOneSizeThread} 1 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs an update-heavy workload with various numbers of size threads and produces the right graph in Figure 12
"python3 measurements/python_scripts/run_java_experiments_scalability.py ${defaultDSSize} 30-20 ${workloadThreadsWithVariableSizeThreads} \"${sizeThreads}\" ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs a read-heavy workload with various numbers of size threads and produces the left graph in Figure 12
"python3 measurements/python_scripts/run_java_experiments_scalability.py ${defaultDSSize} 3-2 ${workloadThreadsWithVariableSizeThreads} \"${sizeThreads}\" ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs an update-heavy workload without a concurrent size thread and produces the top right bars for each data structure comparison in Figure 13
"python3 measurements/python_scripts/run_java_experiments_overhead_split.py ${defaultDSSize} 30-20 \"${workloadThreadsListWithoutSizeThread}\" 0 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs an update-heavy workload with a concurrent size thread and produces the bottom right bars for each data structure comparison in Figure 13
"python3 measurements/python_scripts/run_java_experiments_overhead_split.py ${defaultDSSize} 30-20 \"${workloadThreadsListWithOneSizeThread}\" 1 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs a read-heavy workload without a concurrent size thread and produces the top left bars for each data structure comparison in Figure 13
"python3 measurements/python_scripts/run_java_experiments_overhead_split.py ${defaultDSSize} 3-2 \"${workloadThreadsListWithoutSizeThread}\" 0 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
# Runs a read-heavy workload with a concurrent size thread and produces the bottom left bars for each data structure comparison in Figure 13
"python3 measurements/python_scripts/run_java_experiments_overhead_split.py ${defaultDSSize} 3-2 \"${workloadThreadsListWithOneSizeThread}\" 1 ${warmupRepeats} ${repeats} ${runtime} ${JVMmem} ${shouldRunMeasurements}"
)

for command in "${measurements[@]}"
do
  echo \> "${command}"
  eval ${command}
  date
  echo
done
