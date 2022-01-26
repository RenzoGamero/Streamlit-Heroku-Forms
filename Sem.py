""" """
""" """
import re
from datetime import datetime

StartDate = "09/11/20"
Date = datetime.strptime(StartDate, "%d/%m/%y")
print('Date= ', Date)

# print('Date= ', Date+1)
import datetime

date_1 = datetime.datetime.strptime(StartDate, "%d/%m/%y")
end_date = date_1 + datetime.timedelta(days=0)
print('end_date= ', end_date)
print('After 5 Days:', end_date.strftime('%d/%m__%Y %H:%M:%S'))
ss = str(end_date.strftime('%d/%m'))
print('After 5 Days:', ss)

# replace semana_f2="1) Del 09/11 al 15/11" if semana_f==1
# replace semana_f2="2) Del 16/11 al 22/11" if semana_f==2
# replace semana_f2="3) Del 23/11 al 29/11" if semana_f==3

# from datetime import datetime
StartDate = "09/11/20"
from datetime import datetime

DateO = datetime.strptime(StartDate, "%d/%m/%y")
print('DateO= ', DateO)
import datetime
import pandas as pd

meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
SemanaFull = []
SemanaFull2 = []
IndexFull = []
I_Sem = []
F_Sem = []
I_Mes = []
F_Mes = []
I_Mes_n = []
F_Mes_n = []

df = pd.DataFrame([])
for i in range(200):
    print('----------------------------------------------------------')
    s0 = str(end_date.strftime('%d/%m'))
    s1 = ((end_date + datetime.timedelta(days=6)).strftime('%d/%m'))
    semana = str(i + 1) + ') Del ' + str(s0) + ' al ' + str(s1)

    s00 = str(end_date.strftime('%d/%m/%Y'))
    s11 = ((end_date + datetime.timedelta(days=6)).strftime('%d/%m/%Y'))
    semana2 = str(i + 1) + ') Del ' + str(s00) + ' al ' + str(s11)
    SemanaFull2.append(semana2)
    print(semana)
    print(str(i + 1) + ') Del', s0, ' al ', s1)
    SemanaFull.append(semana)
    IndexFull.append((i + 1))
    dq1 = end_date.strftime('%Y/%m/%d')
    dq2 = ((end_date + datetime.timedelta(days=6)).strftime('%Y/%m/%d'))
    print('dq1= ', dq1)
    print('dq2= ', dq2)

    dq1 = end_date.strftime('%m')
    dq2 = ((end_date + datetime.timedelta(days=6)).strftime('%m'))
    print('dq1_m= ', int(dq1), ' - ', meses[int(dq1) - 1])
    print('dq2_m= ', int(dq2), ' - ', meses[int(dq2) - 1])

    I_Mes.append(meses[int(dq1) - 1])
    F_Mes.append(meses[int(dq2) - 1])

    I_Mes_n.append(int(dq1))
    F_Mes_n.append(int(dq2))

    print(i + 1)

    I_Sem.append(end_date)
    F_Sem.append((end_date + datetime.timedelta(days=6)))

    end_date = (end_date + datetime.timedelta(days=7))

print(SemanaFull)
print(IndexFull)

df['colsem'] = SemanaFull
df['colsem2'] = SemanaFull2
df['semana_f'] = IndexFull

df['I_Sem'] = I_Sem
df['F_Sem'] = F_Sem
df['I_Mes'] = I_Mes
df['F_Mes'] = F_Mes
df['I_Semana_Cr']=df['I_Sem'].dt.week
df['F_Semana_Cr']=df['F_Sem'].dt.week
df['I_year']=df['I_Sem'].dt.year
df['F_year']=df['F_Sem'].dt.year


print(df.head())
print(df.tail())
print(df[['colsem', 'I_Sem', 'F_Sem']].head())
writer = pd.ExcelWriter('Semana.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='Semana', index=False)
writer.save()

import pandas as pd
import numpy as np

workbook_url = 'MejorGasto_Mod_2022-01-25.xlsx'
TabFormularioActual = 'MejorGasto_'
dfRepositorioE = pd.read_excel(workbook_url, sheet_name=TabFormularioActual, engine='openpyxl', keep_default_na=False)
print(dfRepositorioE.head(4))
dft1 = dfRepositorioE

workbook_url = 'Semana.xlsx'
TabFormularioActual = 'Semana'
dfRepositorioE = pd.read_excel(workbook_url, sheet_name=TabFormularioActual, engine='openpyxl', keep_default_na=False)
print(dfRepositorioE)
print(dfRepositorioE.columns)

df = dfRepositorioE


# ['colsem', 'colsem2', 'semana_f', 'I_Sem', 'F_Sem']
# df_start_end = df.melt(id_vars=['colsem', 'colsem2', 'semana_f'],value_name='date')

# df = df_start_end.groupby('Note').apply(lambda x: x.set_index('date').resample('W').pad()).drop(columns=['Note','variable']).reset_index()

def date_expander(dataframe: pd.DataFrame,
                  start_dt_colname: str,
                  end_dt_colname: str,
                  time_unit: str,
                  new_colname: str,
                  end_inclusive: bool):
    td = pd.Timedelta(1, time_unit)

    # add a timediff column:
    dataframe['_dt_diff'] = dataframe[end_dt_colname] - dataframe[start_dt_colname]

    # get the maximum timediff:
    max_diff = int((dataframe['_dt_diff'] / td).max())

    # for each possible timediff, get the intermediate time-differences:
    df_diffs = pd.concat(
        [pd.DataFrame({'_to_add': np.arange(0, dt_diff + end_inclusive) * td}).assign(_dt_diff=dt_diff * td)
         for dt_diff in range(max_diff + 1)])

    # join to the original dataframe
    data_expanded = dataframe.merge(df_diffs, on='_dt_diff')

    # the new dt column is just start plus the intermediate diffs:
    data_expanded[new_colname] = data_expanded[start_dt_colname] + data_expanded['_to_add']

    # remove start-end cols, as well as temp cols used for calculations:
    to_drop = [start_dt_colname, end_dt_colname, '_to_add', '_dt_diff']
    if new_colname in to_drop:
        to_drop.remove(new_colname)
    data_expanded = data_expanded.drop(columns=to_drop)

    # don't modify dataframe in place:
    del dataframe['_dt_diff']

    return data_expanded


dft = date_expander(df, 'I_Sem', 'F_Sem', 'd', 'r', True)
print(dft)

dft['r'] = dft['r'].astype("string")
dft['r'] = pd.Series(dft['r'], dtype="string")
dft['r'] = dft['r'].str[:10]
dft['r2'] = dft['r'].str.replace(r'\D', '')
dft['r2'] = dft.r2.apply(int)

dft1['Fecha'] = dft1['Fecha'].astype("string")
dft1['Fecha'] = pd.Series(dft1['Fecha'], dtype="string")
dft1['Fecha_Full'] = dft1['Fecha']
dft1['Fecha'] = dft1['Fecha'].str[:10]
dft1['Fecha'] = dft1['Fecha'].str.replace(r'\D', '')
dft1['Fecha'] = dft1.Fecha.apply(int)

print('*'*40)
print(dft.head(2))
print(dft.tail(2))
print('*'*40)

print(dft1.head(2))
print(dft1.tail(2))
print('*'*40)

print(dft.dtypes)
print(dft1.dtypes)
"""
"""

dft2 = pd.merge(dft, dft1, left_on='r2', right_on='Fecha', how='right')

print(dft2[['semana_f', 'Fecha']])
