import sys
import os
from shutil import which

import create_graphs as graph

GRAPH_DIR = "graphs"
DATA_DIR = "results"

if len(sys.argv) == 1 or sys.argv[1] == '-h':
  print("Usage: python3 run_java_experiments_overhead_split.py <initSize> <ins>-<del> <workloadThreadsList> <sizeThreads> <num_warmup_repeats> <num_repeats> <runtime> <JVM memory size> <shouldRunMeasurements>")
  print("For example: python3 run_java_experiments_overhead_split.py 10000 3-2 \"[1,4]\" 0 1 1 1 1G T")
  exit(0)

dataStructures = ["BST", "SizeBST", "SkipList", "SizeSkipList", "HashTable", "SizeHashTable"]

initSize = sys.argv[1]
workload = sys.argv[2].split('-')
ins = workload[0]
rmv = workload[1]
if int(ins) + int(rmv) > 100:
  print("ERROR: ins+del must not exceed 100")
  exit(0)
workloadThreadsList = sys.argv[3][1:-1].split(',')
sizeThreads = sys.argv[4]
warmupRepeats = int(sys.argv[5])
repeats = warmupRepeats + int(sys.argv[6])
runtime = sys.argv[7]
JVM_mem_size = sys.argv[8]
shouldRunMeasurements = (sys.argv[9] == 'T')

def delete_previous_results():
  os.system("rm -rf build/*.csv")
  os.system("rm -rf build/*.csv_stdout")

def run_experiments():
  cmdbase = "java -server -Xms" + JVM_mem_size + " -Xmx" + JVM_mem_size + " -jar build/experiments_instr.jar "
  i = 0
  for ds in dataStructures:
    for workloadThreads in workloadThreadsList:
      i = i+1
      if 'Size' in ds:
        sizeThreadsForDs = sizeThreads
      else:
        sizeThreadsForDs = '0'
      cmd = cmdbase + workloadThreads + " " + sizeThreadsForDs + " " + str(repeats) + " " + runtime + " " + ds + " -ins" + ins + " -del" + rmv + " -initSize" + str(initSize) + " -prefill -split -file-build/data-trials" + str(i) + ".csv"
      if os.system(cmd) != 0:
        print("")
        exit(1)

def create_united_results_file():
  os.makedirs(os.path.dirname(results_file_path), exist_ok=True)
  os.system("cat build/data-*.csv > " + results_file_path)

def draw_graphs():
  os.makedirs(GRAPH_DIR, exist_ok=True)

  bars_graph_file_path = os.path.join(GRAPH_DIR, graph_name + "_bars_%s_" + benchmark_name + ".png")
  graph.plot_overhead_bars(results_file_path, bars_graph_file_path, warmupRepeats, True)

graph_name = "overhead_split"
benchmark_name = str(initSize) + "setSize_" + str(ins) + "ins-" + str(rmv) + "rem_" + str(sizeThreads) + "sizeThreads"
results_file_path = os.path.join(DATA_DIR, graph_name + "_" + benchmark_name + ".csv")

if shouldRunMeasurements:
    delete_previous_results()
    run_experiments()
    create_united_results_file()
draw_graphs()