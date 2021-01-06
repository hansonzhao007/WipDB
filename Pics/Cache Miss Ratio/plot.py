import re
import os
import csv
import numpy as np
import pandas as pd
import json
import seaborn as sns
import matplotlib
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from natsort import natsorted, ns
from scipy.fftpack import fft
from matplotlib import gridspec
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import NullFormatter
from matplotlib import rcParams
rcParams['font.family'] = 'Times New Roman'
rcParams['xtick.labelsize']=18
rcParams['ytick.labelsize']=18


fig = plt.figure(figsize=(13, 3)) 
gs = gridspec.GridSpec(9, 9) 
gs.update(wspace=3, hspace=0.5) # set the spacing between axes. 
ax = [1,2,3]
ax[1] = plt.subplot(gs[:, 0:3])
ax[2] = plt.subplot(gs[:, 3:6])
ax[0] = plt.subplot(gs[:, 6:9 ])


def Plot(ax, filename, c, mark):
    df = pd.read_csv(filename)
    df.columns = ["bucket", "speed", "cache_ref", "cache_miss", "tlb_load", "tlb_miss"]
    df['speed'] = df['speed']/ 1024.0
    df['size'] = df["bucket"] * 10000 * 116 / 1024.0 /1024.0
    df = df.set_index('bucket')
    df['cache_miss_ratio'] = df['cache_miss'] / df['cache_ref']
    df['tlb_miss_ratio'] = df['tlb_miss'] * 100.0 / df['tlb_load'] 
    
    df["speed"].plot(ax=ax[0], color=c, marker=mark, markevery=20, markersize=8 )
    df["cache_miss"].plot(ax=ax[1], color=c, marker=mark, markevery=20, markersize=8  )
    df["tlb_miss_ratio"].plot(ax=ax[2], color=c, marker=mark, markevery=20, markersize=8  )
    
    for a in ax:
        a.yaxis.grid(linewidth=1, linestyle='--')
        a.set_axisbelow(True)
        plt.setp(a.xaxis.get_majorticklabels(), ha="right") 
    
    
Plot(ax, "miss_ratio_hash_hugepage.log", "darkgreen", "o")
Plot(ax, "miss_ratio_hash.log", "blue", "^")
Plot(ax, "miss_ratio_skip.log", "orange", "s")
Plot(ax, "miss_ratio_one_skip.log", "grey", '|')

ax[0].set_xlabel('(c) Put Throughput', fontsize=24)
ax[0].set_ylabel('Mops/s', fontsize=24, va='bottom')
ax[0].yaxis.set_label_coords(-0.1,0.5)
# ax[0].set_xticks([])


# ax[1].set_xticks([])
ax[1].set_xlabel('(a) Cache Misses', fontsize=24)
ax[1].set_yscale('log')
ax[1].set_ylabel('Count', fontsize=24, va='bottom')
ax[1].yaxis.set_label_coords(-0.17,0.5)

ax[2].set_xlabel('(b) TLB Misses', fontsize=24)
ax[2].set_ylabel('Percentage (%)', fontsize=24, va='bottom')
ax[2].yaxis.set_label_coords(-0.17,0.5)

ax[1].legend(["Hash-Huge","Hash", "SkipLists", "1-SkipList"], loc="upper right", 
           fontsize=24, edgecolor='k',facecolor='k', framealpha=0, mode="expand", ncol=5,  bbox_to_anchor=(-0.02, 1.18, 3.8, .10))



fig.tight_layout()
plt.savefig('cache_miss.pdf', bbox_inches='tight', pad_inches=0)