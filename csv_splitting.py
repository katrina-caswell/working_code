# Split one CSV into multiple based on a condition, using pyspark

from pyspark.sql.functions import lit, when, col
from pyspark.sql.types import *

path = "..."

spark_df = (spark.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true") 
            .option("sep", ",") 
            .load(path)
           )

display(spark_df)

id_epos = list(spark_df.select(col('EPOS Number')).drop_duplicates().toPandas()['EPOS Number'])

import os
from pyspark.sql.functions import concat, col, lit

for epos in id_epos:

  tbl = spark_df.where(col('EPOS Number') == epos)
  print("Generating file for EPOS {}:   {} rows".format(epos, tbl.count()))
  filename = 'call_list_' + str(epos)
  folder = f'' 

  out_path = os.path.join(folder, filename)
  renamed_file = filename + '.csv' 
  renamed_fullpath = out_path + '.csv' 

  try: 
    dbutils.fs.rm(renamed_fullpath)
  except:
    print('No file to delete')
 
  tbl.coalesce(1).write.csv(out_path, mode='overwrite', header=True)

  file_list = dbutils.fs.ls(out_path)
  for i in file_list:
       file_path = i[0]
       file_name = i[1]
  for i in file_list:
       if i[1].startswith("part-00000"): 
          read_name = i[1]
        # Move it outside to the new_dir folder and rename
          dbutils.fs.mv(os.path.join(out_path,read_name), renamed_fullpath)
        # Remove the empty folder
          dbutils.fs.rm(out_path , recurse = True)
        
# check files in folder
display(dbutils.fs.ls(folder))
