#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
rcParams['font.weight'] = 100
plt.rcParams['xtick.labelsize']=16
plt.rcParams['ytick.labelsize']=16

def ReadBucket(file):
    f = open(file, "r")
    line = f.readline()
    i_num = 1
    buckets = []
    while line:
        if line.startswith("BucketName"):
            line = f.readline()
            b_num = 1
            pivots = []
            sizes  = []
            while line.startswith("/mnt/nvm"):
                data = line.strip().split();
                pivots.append(int(re.findall(r"\d{16}",data[0])[0])) # pivots
                sizes.append(float(data[1])/1024/1024) # size in MB
                line = f.readline()
            buckets.append([pivots, sizes])
        line = f.readline()
    return buckets

df2 = pd.DataFrame(columns = []) 
def ReadFile(name, file):
    wip = pd.DataFrame(columns = ['time', 'thread', 'count', 'speed']) 
    wip_bucket = pd.DataFrame(columns = ['Bucket']) 
    wip_wa = pd.DataFrame(columns = ['Write Amplification']) 
    with open(file) as origin_file:
        for line in origin_file:
            if line.startswith("20"):
                data = line.strip().split()
                data = [data[i] for i in (0,3,4,7)]
                wip.loc[len(wip)] = data
            if line.startswith("BucketCount"):
                data = line.strip().split();
                wip_bucket.loc[len(wip_bucket)] = float(data[1])  
            if line.startswith("WriteAmplification"):
                data = line.strip().split();
                wip_wa.loc[len(wip_wa)] = float(data[1])  
    df2[name] = wip['speed'].apply(extract_current)
    df2[name] = df2[name] / 1000.0 / 1000.0
    df2[name + '_bucket'] = wip_bucket["Bucket"] / 1000
    df2[name + '_wa'] = wip_wa["Write Amplification"]
    

def extract_current(speeds):
    result = re.findall(r"[-+]?\d*\.\d+|\d+", speeds)
    return float(result[0])

def MyPlot(title, name, ax_up, ax_down, wa_pos, is_left_y=False, is_right_y=False, x_title="(a)"):
    def billions(x, pos):
        'The two args are the value and tick position'
        return '%1.0f' % (x/10)
    def ytickformat(x, pos):
        return '%1.1f' % (x)
    def ytickformat2(x, pos):
        return '%1.1f' % (x/1000.0)
    lw=3
    formatter = FuncFormatter(billions)
    formattery = FuncFormatter(ytickformat)
    df2[name].plot(ax=ax_up, fontsize=20, color='steelblue', linewidth=lw)
    ax_up.set_title(title, fontsize=24)
    ax_up.yaxis.grid(linewidth=1, linestyle='--')
    ax_up.set_axisbelow(True)
    ax_up.yaxis.set_major_formatter(formattery)
    ax_up.tick_params(axis="y", labelsize=16)
    major_ticks = np.arange(0, 1.2, 0.2)
    ax_up.set_yticks(major_ticks)
    ax_up.set_ylim([0.01, 1.19])

    ax_up.set_xticks([])
    ax_up.spines["top"].set_visible(False) 
    ax_up.yaxis.set_label_coords(0.06, 1)
    ax_up.tick_params(axis='y', labelcolor='steelblue')
    
    axt = ax_up.twinx();
    axt.spines["top"].set_visible(False) 
    df2[name + '_bucket'].plot(ax=axt, dashes=[2, 2], color='k', markersize=8, linewidth=lw)
    axt.set_ylim([0.01, 1.19])
    axt.tick_params(axis="y", direction="inout", pad=-35)
    start, end = axt.get_ylim()
    axt.yaxis.set_major_formatter(ticker.FormatStrFormatter('%1.1f k'))
    axt.legend(["Number of Buckets"], bbox_to_anchor=(1, -0.02), loc="lower right", fontsize=20, edgecolor='w',framealpha=0)
    
    df2[name + '_wa'].plot(ax=ax_down, linewidth=lw, color='steelblue')
    ax_down.yaxis.grid(linewidth=1, linestyle='--')
    ax_down.set_axisbelow(True)
    ax_down.spines["top"].set_visible(False) 
    ax_down.tick_params(axis='y', labelcolor='steelblue')
    ax_down.xaxis.set_major_formatter(formatter)
    ax_down.set_ylim([2.5, 3.6])
    ax_up.set_xlim([-1,47])
    ax_down.set_xlim([-1,47])
    if(not is_left_y):
        ax_up.set_yticklabels([])
        ax_down.set_yticklabels([])
    else:
        ax_up.set_ylabel( 'Mops/s', fontsize=20, color='steelblue', va='bottom', rotation=0)
        ax_up.yaxis.set_label_coords(0,1.02)
    if(not is_right_y):
        axt.set_yticklabels([])
    else:
        axt.text(36, 1.2, 'Bucket #', fontsize=20, color='k', va='bottom', rotation=0)
        axt.yaxis.set_label_coords(1,1.02)
    # set legend font color same as the line color
    leg = ax_down.legend(["Write Amplification"], bbox_to_anchor=(1, 0),loc="lower right", fontsize=20,framealpha=0.1)

    
    for line, text in zip(leg.get_lines(), leg.get_texts()):
        text.set_color(line.get_color())
    ax_down.set_xlabel(x_title, fontsize=24)
    ax_up.tick_params(axis='both', which='major', labelsize=24)
    ax_down.tick_params(axis='both', which='major', labelsize=24)
    ax_down.xaxis.set_major_locator(ticker.MultipleLocator(10))
    
def PlotHist(ax, hist_data, buckets):
    ax.hist(buckets[0], bins=60, alpha=0.5, edgecolor='black', linewidth=1)
    # ax2 = ax.twinx()
    # ax2.hist(hist_data, bins=100, alpha=0.3, color='red')
    ax.set_xticklabels([])
    # ax.set_yticklabels([])
    # ax2.set_yticks([])
    ax.set_zorder(-1)
    ax.patch.set_visible(False)
    # ys, ym = ax2.get_ylim()
    # ax2.set_ylim([ys, 1.3*ym])
    ax.set_xlim([0, 4000000000])


# In[3]:


logfile = "kv_fillrandom4_2.log"
buckets_i = [0, 2, 4, 6]

buckets = ReadBucket(logfile)
ReadFile("wip4",logfile)
fig = plt.figure(figsize=(12, 7)) 
gs = gridspec.GridSpec(8, 10) 
gs.update(wspace=1, hspace=0.2) # set the spacing between axes. 
ax = [0,0,0,0,0,0]
ax[0] = plt.subplot(gs[0:5,0:5])
ax[1] = plt.subplot(gs[5:8,0:5])

ax[2] = plt.subplot(gs[0:2,5:10])
ax[3] = plt.subplot(gs[2:4,5:10])
ax[4] = plt.subplot(gs[4:6,5:10])
ax[5] = plt.subplot(gs[6:8,5:10])



MyPlot("Throughput",'wip4', ax[0], ax[1], 0, True, True, "Store Size (Billion of Keys)")

hists = []
for f in ['CDF/exponential_50M.txt', 'CDF/normal_50M.txt', 'CDF/uniform_50M.txt', 'CDF/exp_reverse_50M.txt']:
    df = pd.read_csv(f,header=None)
    df.drop(df.columns[1], axis=1, inplace=True)
    hists.append(df[0].tolist())

PlotHist(ax[2], hists[0], buckets[buckets_i[0]])
PlotHist(ax[3], hists[1], buckets[buckets_i[1]])
PlotHist(ax[4], hists[2], buckets[buckets_i[2]])
PlotHist(ax[5], hists[3], buckets[buckets_i[3]])


style = dict(size=14, color='brown')
ax[0].text(1, 0.50, "Exponential", **style, fontsize=12)
ax[0].axvline(10, linestyle='--', color='grey', linewidth=1) # vertical lines
ax[0].text(11, 0.50, "Normal", **style, fontsize=12)
ax[0].axvline(20, linestyle='--', color='grey', linewidth=1) # vertical lines
ax[0].text(21, 0.50, "Uniform", **style, fontsize=12)
ax[0].axvline(30, linestyle='--', color='grey', linewidth=1) # vertical lines
ax[0].text(31, 0.50, "Exponential-Reverse", **style, fontsize=12)

style = dict(size=14, color='k')
ax[2].set_title("Histogram at Different Time", fontsize=24)
ax[2].text(2.8e9, 31, "0.5 Billion", **style, fontsize=20)
ax[3].text(2.8e9, 31, "1.5 Billion", **style, fontsize=20)
ax[4].text(2.8e9, 31, "2.5 Billion", **style, fontsize=20)
ax[5].text(2.8e9, 31, "3.5 Billion", **style, fontsize=20)
shift = 4e9/32
ax[5].text(0 + shift, -10,"Range 1", fontsize=20)
ax[5].text(4e9/4 + shift, -10,"Range 2", fontsize=20)
ax[5].text(4e9/4*2 + shift, -10,"Range 3", fontsize=20)
ax[5].text(4e9/4*3 + shift, -10,"Range 4", fontsize=20)

fig.tight_layout()
plt.savefig("4BInsert2.pdf", bbox_inches='tight')




