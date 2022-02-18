

import pandas as pd

workbook_urlREP='ltr.xlsx'

#all_dfs = pd.read_excel(workbook_urlREP, sheet_name=None)
#print('all_dfs ', all_dfs.keys())
#print('all_dfs ', list(all_dfs.keys()))

workbook_urlDIR = 'Ejemplo de datos.xlsx'
df = pd.read_excel(workbook_urlREP, sheet_name='Hoja 1', header=None)

print(df)
print(df[(df[0] > '2021-09-12')])

df=df[(df[0] > '2021-09-12')]
import re
print('df[2][0]= ', df[2][0])
a1 = re.split(" ", df[2][0])
print(a1)
#df[['left','right']] = df['col'].str.split('[+|-]', expand=True)df[['left','right']] = df['col'].str.split('[+|-]', expand=True)
df[['B1','B2','B3','B4','B5','B6']] = df[2].str.split(' ', expand=True)
print(df)
d=['B1','B2','B3','B4','B5','B6']
for i in range(len(d)):

    df[d[i]] = df[d[i]].astype(int)


print(df)
df=df[['B1','B2','B3','B4','B5','B6']]
#df['DataFrame Column']=0
print('df=== ',df.mode())

#loto=[]
#for i in range(1):
#    loto.append(df[d[1]].tolist())
#print(loto)
#df1=pd.DataFrame(loto, columns=['a'])
#print('df1= ', df1)
#print('df1=== ',df1.mode())
#print(df.pivot_table(index=['DataFrame Column'], aggfunc='size'))
#for i in range(len(d)):
#    print(df.pivot_table(index=[d[i]], aggfunc='size'))

#for i in range(len(d)):
#    df[d[i]].append(df[d[i]]).reset_index(drop=True)
print(pd.Series(df.values.ravel('F')))
print(pd.Series(df.values.ravel('F')).mode())
print(pd.Series(df.values.ravel('F')).value_counts())


print(pd.DataFrame(pd.Series(df.values.ravel('F'))))
dfr= pd.DataFrame(pd.Series(df.values.ravel('F')))
print('dfr= ', dfr)
print(pd.DataFrame(pd.Series(df.values.ravel('F'))).mode())
print('dfr= ', dfr[0].mode())
print(pd.DataFrame(pd.Series(df.values.ravel('F'))).value_counts())
