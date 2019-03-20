import pandas as pd
import pandas_profiling
import matplotlib
import matplotlib.pyplot as plt
import scipy.stats as st
import numpy as np
from sqlalchemy import create_engine,MetaData,Table,select,engine
import pandas_profiling
import datetime

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

DB_PROD={
        'drivername':   'oracle+cx_oracle',       
        'servername':   '10.100.3.76',    
        'port': '1521',    
        'sidname':'DMPRD',    
        'username': 'ODI_UNICOMER', 
        'password': 'Unicomer$01'
        }
engine_test=create_engine(DB_PROD['drivername'] + '://' + DB_PROD['username']+':' + DB_PROD['password'] + '@' + DB_PROD['servername'] + ':' + DB_PROD['port'] + '/' + DB_PROD['sidname'])


qryMarcaSrv="""SELECT DISTINCT MARCA_RI_NORM MARCA_SERVICES FROM
   (SELECT /*+MATERIALIZE*/
      MO.COUNTRY_CODE,
      MO.MODELO_ARTICULO_ID ID_SRV,
      MO.MODELO_ARTICULO MODELO_SRV,
      REGEXP_REPLACE(UPPER(MO.MODELO_ARTICULO),'\W+','') MODELO_SRV_NORM,
      MO.DESC_ARTICULO DESCRIPCION_SRV,
      REGEXP_REPLACE(UPPER(MO.DESC_ARTICULO),'\W+','') DESCRIPCION_SRV_NORM,
      MA.DESC_MARCA_ARTICULO MARCA_SRV,
      REGEXP_REPLACE(UPPER(MA.DESC_MARCA_ARTICULO),'\W+','') MARCA_SRV_NORM,      
      TRIM(
          REGEXP_REPLACE(
            UPPER(REGEXP_REPLACE(MA.DESC_MARCA_ARTICULO,' *\(.*$','')),
            '\W+'
          )
      ) MARCA_RI_NORM
    FROM ODS.TLR_MODELOS_ARTICULOS MO
      LEFT JOIN ODS.TLR_MARCAS_ARTICULOS MA ON MO.MARCA_ARTICULO_ID = MA.MARCA_ARTICULO_ID)
      ORDER BY 1 ASC
"""

qryMarcaRI="SELECT DISTINCT BRAND_DESC MARCA FROM DIM.DW_DIM_PRODUCT_BRAND ORDER BY MARCA ASC"

names_array=[]
ratio_array=[]
def match_names(wrong_names,correct_names):
    for row in wrong_names:
        x=process.extractOne(row,correct_names,scorer=fuzz.ratio)    
        names_array.append(x[0])
        ratio_array.append(x[1])
    return names_array,ratio_array

#Wrong marca dataset
dfMarcaSrv=pd.read_sql_query(qryMarcaSrv,engine_test)
wrong_names=dfMarcaSrv["marca_services_norm"].dropna().values

#Correct Marca dataset
dfMarcaRI=pd.read_sql_query(qryMarcaRI,engine_test)
correct_names=dfMarcaRI["marca"].values


name_match,ratio_match=match_names(wrong_names,correct_names)

dfMarcaSrv["correct_marca_name"]=pd.Series(name_match)
dfMarcaSrv["marca_name_ratio"]=pd.Series(ratio_match)

dfMarcaSrv.to_csv("String_Matched_Marcas_Name.csv")
