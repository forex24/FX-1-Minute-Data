import utils
import os
from zipfile import ZipFile
import pandas as pd

def get_subdirectories(directory):
    subdirectories = []
    for root, dirs, files in os.walk(directory):
        for dir in dirs:
            subdirectories.append(os.path.join(root, dir))
    return subdirectories

csv_cols= ['dt', 'open', 'high', 'low', 'close', 'vol']

def get_symbol_parquet(path):
  symbol = os.path.basename(path)
  path_list=os.listdir(path)
  df_list = []
  for filename in path_list:
    if os.path.splitext(filename)[1]==".zip": 
    	# splitext会把文件名拆成两部分，第一部分是文件名，第二部分是后缀名
    	# 不管有多少个点，都是只把最后一个点以及后面的字符串当作后缀名
    	# 比如文件名为a.b.c，会把"a.b"当作文件名，".c"当作后缀名
        full_path = os.path.join(path,filename)
        zipfile = ZipFile(full_path)
        csv_name = filename.replace(".zip",".csv") 
        print(csv_name)
        csv_file = zipfile.open(csv_name)
        df = pd.read_csv(csv_file, names=csv_cols, index_col=None, sep=';')
        df['timestamp'] = pd.to_datetime(df['dt'])
        df.drop(['dt'], axis=1, inplace=True)
        csv_file.close()
        zipfile.close()
        df_list.append(df)
  df_all = pd.concat(df_list, ignore_index=True)
  df_all = df_all.sort_values(by='timestamp')
  #df_all['timestamp'] = pd.to_datetime(df['dt'])
  print("write:" + symbol)
  df_all.to_parquet(symbol+'.parquet')



root_path = 'output'
sub_dirs = get_subdirectories(root_path)
for path in sub_dirs:
  get_symbol_parquet(path)
