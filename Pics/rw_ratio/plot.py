#!/usr/bin/env python
# coding: utf-8

# In[65]:


# libraries
import numpy as np
import pandas as pd
import os
import re
import matplotlib.pyplot as plt
from operator import truediv
from matplotlib.pyplot import figure
from matplotlib import gridspec
from matplotlib.ticker import FuncFormatter

from matplotlib import rcParams
rcParams['font.family'] = 'Times New Roman'
rcParams['hatch.linewidth'] = 0.1  # previous pdf hatch linewidth
rcParams['hatch.linewidth'] = 1.0  # previous svg hatch linewidth

import matplotlib.pyplot as plt




df = pd.read_csv("res.txt")

df = df.set_index("workload")
df = df[["kv", "level", "peb", "rocks"]]
fig = plt.figure(figsize=(12, 4))
ax = fig.add_subplot(111)
df.plot.bar(ax=ax, colormap='Pastel2', width=0.75, edgecolor='k', fontsize=16)


bars = ax.patches
hatches = ''.join(h*len(df) for h in 'x *o')
for bar, hatch in zip(bars, hatches):
    bar.set_hatch(hatch)

    

ax.legend(["WiPDB", "LevelDB", "PebDB", "RocksDB"], loc="upper right", fontsize=23, 
          edgecolor='k',facecolor='w', framealpha=0, mode="expand", ncol=4, bbox_to_anchor=(0, 1.12, 1.02, .101))

ax.set_ylabel("Operations / Second", fontsize=20)
ax.set_xlabel("", fontsize=20)
ax.set_xticklabels(["W100R0", "W80R20", "W50R50", "W25R75", "W0R100"], rotation=0, fontsize=20)
ax.yaxis.grid(True, color ="grey", zorder=-1, dashes=(4,4))
ax.set_axisbelow(True)
fig.savefig("rw_ratio.pdf", bbox_inches='tight')


# In[ ]:





# In[ ]:





# In[ ]:




