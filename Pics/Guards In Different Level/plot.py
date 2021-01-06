import re
import os, psutil, numpy as np
import pandas as pd
import json
from datetime import datetime
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['font.family'] = 'Times New Roman'
rcParams['font.weight'] = 100
plt.rcParams['xtick.labelsize']=16
plt.rcParams['ytick.labelsize']=16

L1 = pd.read_csv("L1.csv").iloc[:, 1:].drop_duplicates()
print("L1 compaction record(recover):", len(L1))

L2 = pd.read_csv("L2.csv").iloc[:, 1:].drop_duplicates()
print("L2 compaction record(recover):", len(L2))

L3 = pd.read_csv("L3.csv").iloc[:, 1:].drop_duplicates()
print("L3 compaction record(recover):", len(L3))

# L4 = pd.read_csv("L4.csv").iloc[:, 1:].drop_duplicates()
# print("L4 compaction record(recover):", len(L4))

def DropAjacentRowHasDuplicate(L):
#     return the index of unique rows
    last = []
    rows = []
    res = []
    for index, row in L.iterrows():
        cur = row.tolist()
        common = len(set(cur) & set(last))
        if common == 0:
            res.append(index)
        last = cur
    return res

def FilterFixNoTableRow(L, count):
    LCount = L.notnull().sum(axis=1)
    res = LCount[LCount == count]
    dedup_index = DropAjacentRowHasDuplicate(L.loc[res.index])
    return L.loc[dedup_index]

f = plt.figure(figsize=(10,6))
ax1 = f.add_subplot(311)
ax2 = f.add_subplot(312)
ax3 = f.add_subplot(313)

(L1.loc[5000:12000:10, ::5]/1e9 * 100.0).plot.line(ax=ax1, legend=False, linewidth=1, cmap='gist_rainbow')
plt.setp(ax1.get_xticklabels(), visible=False)

(L2.loc[5000:12000:10, ::20]/1e9 * 100.0).plot.line(ax=ax2, legend=False, linewidth=1,cmap='gist_rainbow')
plt.setp(ax2.get_xticklabels(), visible=False)

(L3.loc[5000:12000:10, ::60]/1e9 * 100.0).plot.line(ax=ax3, legend=False, linewidth=1,cmap='gist_rainbow')

ax1.set_ylabel("Level 1", fontsize=16 )
ax2.set_ylabel("Level 2", fontsize=16 )
ax3.set_ylabel("Level 3", fontsize=16 )
plt.tight_layout()
# plt.ylabel("Guard Value", fontsize=24)
f.text(0.05, 1, '%', ha='center', size=16)
f.text(0.5, - 0.04, 'Number of Compactions', ha='center', size=24)
f.text(-0.03, 0.35, 'Guard Position', va='bottom', rotation='vertical', size=24)

plt.savefig('L3.pdf', bbox_inches='tight')