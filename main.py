#%%
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from ydata_profiling import ProfileReport

#%%
requests = pd.read_csv('FSU_Gordon/SD_FAULT_REFERENCE.csv', sep=';',encoding='latin1')
aprrovals = dataframe = pd.read_csv('FSU_Gordon/SD_FAULT_REFERENCE_REQUEST.csv', sep=';',encoding='latin1')

faults = pd.read_csv('FSU_Gordon/SD_FAULT.csv', sep=',',encoding='latin1',on_bad_lines="warn")

#%%
faults = pd.read_csv('copa/EVT_EVENT.csv', sep=',',encoding='latin1',on_bad_lines="warn")
# %%
aprrovals.head()
# %%
requests.head()

#%%
display(faults["CREATION_DT"])
# %%
print(faults.columns.tolist())

# %%
faults["FAULT_ID"].unique()

#%%
profile = ProfileReport(requests, title="Profiling Report")
# %%
profile.to_file("EVT_EVENT_report.html")

#%%

fails = pd.read_csv('copa/FAIL_MODE.csv', sep=';',encoding='latin1',on_bad_lines="warn")
#%%
display(fails["CREATION_DT"])

#%%
display(aprrovals["CREATION_DT"][::100])
# %%
print(requests["NOTES"].unique()[:100])

# %%
print(fails["MTBF_QT"])

#%%
failsdeferref = pd.read_csv('copa/FAIL_DEFER_REF.csv', sep=';',encoding='latin1',on_bad_lines="warn")

# %%
print(failsdeferref["PERF_PENALTIES_LDESC"].unique())