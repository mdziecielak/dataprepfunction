import pandas as pd
import numpy as np
file= "monthly_report_full-2022-11-65fbdfb3-0ebd-48b9-9207-1f37b6dc91f8.csv"
filere= file.split("-")

month= filere[2]
year= filere[1]

mf=pd.read_csv("python/klaritylongterm/"+file)
cmf=mf.dropna(subset=['Provider'])
cmf['Month']=month
cmf['Year']=year
header = ["Provider", "Resource ID", "Monthly cost", "Application name", "Month", "Year"]
filenameout="klarity_monthly_"+year+month

print(type(file))
cmf.to_csv(filenameout+'.csv', columns= header, index=False)