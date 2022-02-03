import pandas as pd
import os
#from glob import glob
import glob
excel_files = glob.glob('/Users/renzogamero 1 2 3/Downloads/Modulos/*.csv')
print('inicio')
for csv_file in excel_files:
#for csv_file in os.listdir("/Users/renzogamero 1 2 3/Downloads/Modulos"):
    #print('csv_file=', csv_file)
    #if csv_file.endswith('.csv'):
        print('---------------------------------------------------------------')
        print(csv_file)
        df = pd.read_csv(csv_file,sep=';')
        xlsx_file = os.path.splitext(csv_file)[0] + '.xlsx'
        df.to_excel(xlsx_file, index=None, header=True)