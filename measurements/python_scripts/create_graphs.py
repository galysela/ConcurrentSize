import csv
import matplotlib as mpl
mpl.use('Agg')
mpl.rcParams['grid.linestyle'] = ":"
mpl.rcParams['grid.color'] = "black"
mpl.rcParams.update({'font.size': 15})
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np
import os
import statistics as st

import enum

class WorkloadOpType(enum.Enum):
    all = -1.5
    insert = -0.5
    delete = 0.5
    contains = 1.5

areGraphsForPaper = False # To set hard-coded graph y limits as in the paper

columns = ['name', 'nWorkloadThreads', 'nSizeThreads', 'percentageRatio', 'initSize', 'time', 'workloadThreadsThroughput', 'sizeThreadsThroughput', 'ninstrue', 'ninsfalse', 'ndeltrue', 'ndelfalse', 'ncontainstrue', 'ncontainsfalse', 'totalelapsedinstime', 'totalelapseddeltime', 'totalelapsedcontainstime']

names = { 'BST': 'BST',
          'SizeBST': 'SizeBST',

          'SkipList': 'SkipList',
          'SizeSkipList': 'SizeSkipList',

          'HashTable': 'HashTable',
          'SizeHashTable': 'SizeHashTable',

          'IteratorSkipList': 'SnapshotSkipList',

          'VcasBatchBSTGC64': 'VcasBST-64',
}
colors = {
          'BST': 'C3',
          'SizeBST': 'C1',

          'SkipList': 'C0',
          'SizeSkipList': 'C2',

          'HashTable': 'C4',
          'SizeHashTable': 'C6',

          'IteratorSkipList': 'C9',

          'VcasBatchBSTGC64': 'C5',
}
splitColors = {
               WorkloadOpType.all: 'C9',
               WorkloadOpType.insert: 'C7',
               WorkloadOpType.delete: 'pink',
               WorkloadOpType.contains: 'C4',
}
linestyles = {
              'BST': '--',
              'SizeBST': '-',

              'SkipList': '--',
              'SizeSkipList': '-',

              'HashTable': '--',
              'SizeHashTable': '-',

              'IteratorSkipList': ':',

              'VcasBatchBSTGC64': ':',
}
markers =    {
              'BST': 'x',
              'SizeBST': 'X',

              'SkipList': '+',
              'SizeSkipList': 'P',

              'HashTable': '.',
              'SizeHashTable': 'o',

              'IteratorSkipList': '2',

              'VcasBatchBSTGC64': '*',
}
hatches = {
           'SizeBST' : 'x',
           'SizeHashTable' : 'O',
           'SizeSkipList' : '+',
}
# predetermined order for the graph legends
algs_order = ["HashTable", "SizeHashTable", "BST", "SizeBST", "SkipList", "SizeSkipList", "VcasBatchBSTGC64", "IteratorSkipList"]

def toRatio(insert, delete, size):
  return str(insert) + 'i-' + str(delete) + 'd-' + str(size) + 'size'

def toString(algname, workloadThreadsNum, sizeThreadsNum, initSize, percentageRatio):
  return algname + '-' + str(workloadThreadsNum) + 'w-' + str(sizeThreadsNum) + 's-' + str(initSize) + 'k-' + percentageRatio

def toStringSplit(algname, workloadThreadsNum, sizeThreadsNum, initSize, percentageRatio, workloadOpType):
  return toString(algname, workloadThreadsNum, sizeThreadsNum, initSize, percentageRatio) + 'r-' + workloadOpType.name

def avg(numlist):
  total = 0.0
  length = 0
  for num in numlist:
    length=length+1
    total += float(num)
  if length > 0:
    return 1.0*total/length
  else:
    return -1

def readJavaResultsFile(path, results, stddev, workloadThreads, sizeThreads, ratios, initSizes, algs, warmupRepeats, isWorkloadThreadsTP, isSplitByOpType=False):
  columnIndex = {}
  resultsRaw = {}

  # read csv into resultsRaw
  with open(path, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in csvreader:
      if not bool(columnIndex): # columnIndex dictionary is empty
        for col in columns:
          columnIndex[col] = row.index(col)
      if row[columnIndex[columns[0]]] == columns[0]:  # row contains column titles
        continue
      values = {}
      for col in columns:
        values[col] = row[columnIndex[col]]
      if int(values['nWorkloadThreads']) not in workloadThreads:
        workloadThreads.append(int(values['nWorkloadThreads']))
      if 'Size' in values['name'] and int(values['nSizeThreads']) not in sizeThreads:
        sizeThreads.append(int(values['nSizeThreads']))
      if int(values['initSize']) not in initSizes:
        initSizes.append(int(values['initSize']))
      if values['percentageRatio'] not in ratios:
        ratios.append(values['percentageRatio'])
      if values['name'] not in algs:
        algs.append(values['name'])
      time = float(values['time'])

      if not isSplitByOpType:
        key = toString(values['name'], values['nWorkloadThreads'], values['nSizeThreads'], values['initSize'], values['percentageRatio'])
        if key not in resultsRaw:
          resultsRaw[key] = []
        if isWorkloadThreadsTP:
          resultsRaw[key].append(int(values['workloadThreadsThroughput']))
        else:
          resultsRaw[key].append(int(values['sizeThreadsThroughput']))
      else:
        for workloadOpType in WorkloadOpType:
          key = toStringSplit(values['name'], values['nWorkloadThreads'], values['nSizeThreads'], values['initSize'], values['percentageRatio'], workloadOpType)
          if key not in resultsRaw:
            resultsRaw[key] = []
          if workloadOpType == WorkloadOpType.all:
            resultsRaw[key].append((int(values['ninstrue'])+int(values['ninsfalse'])+int(values['ndeltrue'])+int(values['ndelfalse'])+int(values['ncontainstrue'])+int(values['ncontainsfalse']))/(float(values['totalelapsedinstime'])+float(values['totalelapseddeltime'])+float(values['totalelapsedcontainstime'])))
          elif workloadOpType == WorkloadOpType.insert:
            resultsRaw[key].append((int(values['ninstrue'])+int(values['ninsfalse']))/float(values['totalelapsedinstime']))
          elif workloadOpType == WorkloadOpType.delete:
            resultsRaw[key].append((int(values['ndeltrue'])+int(values['ndelfalse']))/float(values['totalelapseddeltime']))
          else: # workloadOpType == WorkloadOpType.contains
            resultsRaw[key].append((int(values['ncontainstrue'])+int(values['ncontainsfalse']))/float(values['totalelapsedcontainstime']))

  if isWorkloadThreadsTP:
    divideBy = 1000000.0
  else:
    divideBy = 1000.0

  with open(os.path.join(os.path.dirname(path), os.path.basename(path)[:-len('.csv')] + '_statistics.csv'), 'w', newline='') as statisticsFile:
    writer = csv.writer(statisticsFile)
    writer.writerow(['benchmark', 'meanTP', 'stddev', 'CV'])
    for key in resultsRaw:
      resultsAll = resultsRaw[key]
      resultsExcludingWarmup = resultsAll[warmupRepeats:]
      results[key] = avg(resultsExcludingWarmup)
      stddev[key] = st.pstdev(resultsExcludingWarmup)
      if results[key] < 1e-8:
        CV = -1
      else:
        CV = stddev[key] / results[key]
      writer.writerow([key, "%.3f" % results[key], "%.3f" % stddev[key], "%.3f" % CV])
      if not isSplitByOpType:
        results[key] /= divideBy

def plot_overhead_bars(input_file_path, output_graph_path, warmupRepeats, isSplitByOpType):
  results = {}
  stddev = {}
  workloadThreads = []
  sizeThreads = []
  ratios = []
  initSizes = []
  algs = []

  readJavaResultsFile(input_file_path, results, stddev, workloadThreads, sizeThreads, ratios, initSizes, algs, warmupRepeats, True, isSplitByOpType)
  assert(len(sizeThreads) == 1)
  sizeThreadsNum = sizeThreads[0]
  workloadThreads.sort()
  assert(len(initSizes) == 1)
  initSize = initSizes[0]
  assert(len(ratios) == 1)
  percentageRatio = ratios[0]

  for sizeAlg in algs:
    if 'Size' not in sizeAlg:
      continue
    if not isSplitByOpType:
      plot_non_split_overhead_bars(results, workloadThreads, sizeThreadsNum, percentageRatio, initSize, sizeAlg, output_graph_path)
    else:
      plot_split_overhead_bars(results, workloadThreads, sizeThreadsNum, percentageRatio, initSize, sizeAlg, output_graph_path)

def plot_non_split_overhead_bars(results, workloadThreads, sizeThreadsNum, percentageRatio, initSize, sizeAlg, output_graph_path):
  baselineAlg = sizeAlg[len('Size'):]
  series = []
  for th in workloadThreads:
    key = toString(sizeAlg, th, sizeThreadsNum, initSize, percentageRatio)
    baselineKey = toString(baselineAlg, th, 0, initSize, percentageRatio)
    if key in results:
      series.append(100 - results[key]/results[baselineKey]*100)

  maxValue = max(max(series), 10)
  minValue = min(min(series), 0)

  fig, axs = plt.subplots(figsize=(6.5, (maxValue-minValue+2)/21*1.8))
  opacity = 0.8
  min_diff = min([j-i for i, j in zip(workloadThreads[:-1], workloadThreads[1:])])
  width = 0.75 * min_diff  # the width of the bars

  rects = axs.bar(workloadThreads, series, width, label=names[sizeAlg], color=colors[sizeAlg])

  axs.set_xticks(workloadThreads)
  plt.tick_params(axis='x', bottom=False, top=True, labelbottom=False, labeltop=True)
  TP_loss_percentages = np.arange(20)*10-100
  TP_ratio_percentages = 100 - TP_loss_percentages
  axs.set_yticks(TP_loss_percentages)
  axs.set_yticklabels(TP_ratio_percentages)
  ylabel = '% TP loss'
  axs.set(ylabel=ylabel)
  axs.set_ylim(bottom=minValue, top=maxValue+2)
  axs.invert_yaxis()
  axs.spines['bottom'].set_visible(False)
  axs.spines['right'].set_visible(False)
  axs.spines['left'].set_visible(False)
  axs.spines['top'].set_visible(False)
  axs.set_axisbelow(True)
  yLineValue = -100
  while yLineValue <= maxValue + 2:
    plt.axhline(y=yLineValue, linewidth=1, color='k', linestyle='--' if yLineValue != 0 else '-')
    yLineValue += 10
  path = output_graph_path % sizeAlg
  plt.savefig(path, bbox_inches='tight', dpi=300)
  plt.close('all')

def plot_split_overhead_bars(results, workloadThreads, sizeThreadsNum, percentageRatio, initSize, sizeAlg, output_graph_path):
  baselineAlg = sizeAlg[len('Size'):]
  fig, axs = plt.subplots()
  maxValue = 10
  minValue = 0
  x = np.arange(len(workloadThreads))
  for workloadOpType in WorkloadOpType:
    series = []
    for th in workloadThreads:
      key = toStringSplit(sizeAlg, th, sizeThreadsNum, initSize, percentageRatio, workloadOpType)
      baselineKey = toStringSplit(baselineAlg, th, 0, initSize, percentageRatio, workloadOpType)
      if key in results:
        series.append(100 - results[key]/results[baselineKey]*100)

    maxValue = max(maxValue, max(series))
    minValue = min(minValue, min(series))

    opacity = 0.8
    width = 0.2  # the width of the bars

    rects = axs.bar(x + workloadOpType.value*width, series, width, label=workloadOpType.name, color=splitColors[workloadOpType])

  axs.set_xticks(x)
  axs.set_xticklabels(workloadThreads)
  plt.tick_params(axis='x', bottom=False, top=True, labelbottom=False, labeltop=True)
  TP_loss_percentages = np.arange(20)*10-100
  TP_ratio_percentages = 100 - TP_loss_percentages
  axs.set_yticks(TP_loss_percentages)
  axs.set_yticklabels(TP_ratio_percentages)
  ylabel = '% TP loss'
  axs.set(ylabel=ylabel)
  axs.set_ylim(bottom=minValue, top=maxValue+2)
  axs.invert_yaxis()
  axs.spines['bottom'].set_visible(False)
  axs.spines['right'].set_visible(False)
  axs.spines['left'].set_visible(False)
  axs.spines['top'].set_visible(False)

  legend_x = 1
  legend_y = 0.5
  legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=4)
  export_legend(legend, "graphs/legend_overhead_split_bars.png")
  legend.remove()

  axs.set_axisbelow(True)
  yLineValue = -100
  while yLineValue <= maxValue + 2:
    plt.axhline(y=yLineValue,linewidth=1, color='k', linestyle='--' if yLineValue != 0 else '-')
    yLineValue += 10
  path = output_graph_path % sizeAlg
  fig.set_size_inches(6.5, (maxValue-minValue+2)/21*1.8)
  plt.savefig(path, bbox_inches='tight', dpi=300)
  plt.close('all')

def plot_overhead_graphs(input_file_path, output_graph_path, warmupRepeats):
  throughput = {}
  stddev = {}
  workloadThreads = []
  sizeThreads = []
  ratios = []
  initSizes = []
  algs = []

  readJavaResultsFile(input_file_path, throughput, stddev, workloadThreads, sizeThreads, ratios, initSizes, algs, warmupRepeats, True)
  assert(len(sizeThreads) == 1)
  sizeThreadsNum = sizeThreads[0]
  workloadThreads.sort()
  assert(len(initSizes) == 1)
  initSize = initSizes[0]
  assert(len(ratios) == 1)
  percentageRatio = ratios[0]

  for sizeAlg in algs:
    if 'Size' not in sizeAlg:
      continue
    baselineAlg = sizeAlg[len('Size'):]
    series = {}
    error = {}
    ymax = 0
    series[sizeAlg] = []
    error[sizeAlg] = []
    series[baselineAlg] = []
    error[baselineAlg] = []

    for th in workloadThreads:
      key = toString(sizeAlg, th, sizeThreadsNum, initSize, percentageRatio)
      baselineKey = toString(baselineAlg, th, 0, initSize, percentageRatio)
      assert key in throughput
      assert baselineKey in throughput
      series[sizeAlg].append(throughput[key])
      error[sizeAlg].append(stddev[key])
      series[baselineAlg].append(throughput[baselineKey])
      error[baselineAlg].append(stddev[baselineKey])

    fig, axs = plt.subplots(figsize=(6.5, 4.2))
    opacity = 0.8
    rects = {}

    for alg in (baselineAlg, sizeAlg):
      ymax = max(ymax, max(series[alg]))
      rects[alg] = axs.plot(workloadThreads, series[alg],
        alpha=opacity,
        color=colors[alg],
        linestyle=linestyles[alg],
        linewidth=3,
        marker=markers[alg],
        markersize=10,
        label=names[alg])

    if areGraphsForPaper:
      if 'HashTable' in alg:
        ytop=88
      elif 'BST' in alg:
        ytop=30
      else:
        ytop=17
      axs.set_ylim(bottom=-0.02*ytop, top=ytop)
    else:
      axs.set_ylim(bottom=-0.02*ymax)

    plt.xticks(workloadThreads, workloadThreads)
    axs.set(xlabel='Workload threads', ylabel='Workload threads total TP (Mop/s)')
    legend_x = 1
    legend_y = 0.5
    legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=len(rects))
    export_legend(legend, "graphs/legend_overhead_lines_%s.png" % sizeAlg)
    legend.remove()

    plt.grid()
    axs.set_axisbelow(True)
    plt.savefig(output_graph_path % sizeAlg, bbox_inches='tight', dpi=300)
    plt.close('all')

def plot_scalability_graph(input_file_path, output_graph_path, warmupRepeats):
  throughput = {}
  stddev = {}
  workloadThreads = []
  sizeThreads = []
  ratios = []
  initSizes = []
  algs = []

  readJavaResultsFile(input_file_path, throughput, stddev, workloadThreads, sizeThreads, ratios, initSizes, algs, warmupRepeats, False)
  assert(len(workloadThreads) == 1)
  workloadThreadsNum = workloadThreads[0]
  sizeThreads.sort()

  ymax = 0
  series = {}
  error = {}
  for alg in algs:
    series[alg] = []
    error[alg] = []
    for th in sizeThreads:
      if toString(alg, workloadThreadsNum, th, initSizes[0], ratios[0]) not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[toString(alg, workloadThreadsNum, th, initSizes[0], ratios[0])])
      error[alg].append(stddev[toString(alg, workloadThreadsNum, th, initSizes[0], ratios[0])])

  fig, axs = plt.subplots(figsize=(6.5, 4.2))
  opacity = 0.8
  rects = {}

  for alg in algs_order:
    if not alg in series:
      continue
    ymax = max(ymax, max(series[alg]))
    rects[alg] = axs.plot(sizeThreads, series[alg],
      alpha=opacity,
      color=colors[alg],
      linestyle=linestyles[alg],
      linewidth=3,
      marker=markers[alg],
      markersize=10,
      label=names[alg])

  if areGraphsForPaper:
    ytop=280
    axs.set_ylim(bottom=-0.02*ytop, top=ytop)
  else:
    axs.set_ylim(bottom=-0.02*ymax)

  plt.xticks(sizeThreads, sizeThreads)
  axs.set(xlabel='Size threads', ylabel='Size threads total TP (Kop/s)')

  legend_x = 1
  legend_y = 0.5
  legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=len(rects))
  export_legend(legend, "graphs/legend_scalability.png")
  legend.remove()

  plt.grid()
  axs.set_axisbelow(True)
  plt.savefig(output_graph_path, bbox_inches='tight', dpi=300)
  plt.close('all')

def plot_per_size_graph(input_file_path, output_graph_path, warmupRepeats, showSizeAlgs):
  throughput = {}
  stddev = {}
  workloadThreads = []
  sizeThreads = []
  ratios = []
  initSizes = []
  algs = []

  readJavaResultsFile(input_file_path, throughput, stddev, workloadThreads, sizeThreads, ratios, initSizes, algs, warmupRepeats, False)
  initSizes.sort()
  assert(len(workloadThreads) == 1)
  workloadThreadsNum = workloadThreads[0]
  assert(len(sizeThreads) == 1)
  sizeThreadsNum = sizeThreads[0]

  ymax = 0
  series = {}
  error = {}
  for alg in algs:
    if ('Size' in alg and not showSizeAlgs) or ('Size' not in alg and showSizeAlgs):
      continue 
    series[alg] = []
    error[alg] = []
    for initSize in initSizes:
      if toString(alg, workloadThreadsNum, sizeThreadsNum, initSize, ratios[0]) not in throughput:
        del series[alg]
        del error[alg]
        break
      series[alg].append(throughput[toString(alg, workloadThreadsNum, sizeThreadsNum, initSize, ratios[0])])
      error[alg].append(stddev[toString(alg, workloadThreadsNum, sizeThreadsNum, initSize, ratios[0])])

  fig, axs = plt.subplots(figsize=(6.5, 4.2))
  opacity = 0.8
  rects = {}

  for alg in algs_order:
    if not alg in series:
      continue
    if ('Size' in alg and not showSizeAlgs) or ('Size' not in alg and showSizeAlgs):
      continue
    ymax = max(ymax, max(series[alg]))
    rects[alg] = axs.plot(initSizes, series[alg],
      alpha=opacity,
      color=colors[alg],
      linestyle=linestyles[alg],
      linewidth=3,
      marker=markers[alg],
      markersize=10,
      label=names[alg])

  if areGraphsForPaper:
    if showSizeAlgs:
      ytop=100
    else:
      ytop=0.265
    axs.set_ylim(bottom=-0.02*ytop, top=ytop)
  else:
    axs.set_ylim(bottom=-0.02*ymax, top=1.05*ymax)

  TPStr = 'threads total TP'
  if sizeThreadsNum == 1: # a single size thread
    TPStr = 'thread TP'
  axs.set(xlabel='Data structure size', ylabel='Size '+TPStr+' (Kop/s)')
  x_base = int(initSizes[1]/initSizes[0])
  if x_base == 10:
    axs.set_xscale('log')
    axs.set_xticks(initSizes)
  else:
    axs.set_xscale('log', base=x_base)
    axs.set_xticks(initSizes)
    axs.set_xticklabels(["%.e" % s for s in initSizes])

  legend_name = "legend_per_size"
  if not showSizeAlgs:
    legend_name += "_others"
  legend_x = 1
  legend_y = 0.5
  legend = plt.legend(loc='center left', bbox_to_anchor=(legend_x, legend_y), ncol=len(rects))
  export_legend(legend, "graphs/" + legend_name + ".png")
  legend.remove()

  plt.grid()
  axs.set_axisbelow(True)
  plt.savefig(output_graph_path, bbox_inches='tight', dpi=300)
  plt.close('all')

def export_legend(legend, filename):
    fig = legend.figure
    fig.canvas.draw()
    bbox = legend.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(filename, dpi=300, bbox_inches=bbox)
