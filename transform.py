from pyspark.sql.types import IntegerType, FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import findspark
import pandas as pd
import pyodbc
import os

os.environ['PYSPARK_PYTHON'] = 'python'
findspark.init()

spark = SparkSession.builder \
    .master('local') \
    .appName('Pandas') \
    .getOrCreate()

def conexao_db(banco):
    host = '127.0.0.1'
    login = 'tass'
    pwd = 'Tulio2023'
    try:
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                              host + ';DATABASE=' + banco + ';UID=' + login + ';PWD=' + pwd)
    except pyodbc.InterfaceError as e:
        return f'Erro de conexão - {e}'
    else:
        return cnxn

def carrega_carga_fato():
    with conexao_db('BRONZE') as cnxn_bronze, conexao_db('GOLD') as cnxn_gold:
        cursor_destino = cnxn_gold.cursor()
        query = 'SELECT * FROM TBL_CARGA'
        pdf = pd.read_sql(query, cnxn_bronze)
        dfBronze = spark.createDataFrame(pdf)

        dfGold = dfBronze.withColumn('IDCARGA', dfBronze.IDCARGA.cast(IntegerType())) \
            .withColumn('IDATRACACAO', dfBronze.IDATRACACAO.cast(IntegerType())) \
            .withColumn('TEU', dfBronze.TEU.cast(IntegerType())) \
            .withColumn('QTCARGA', dfBronze.QTCARGA.cast(IntegerType())) \
            .withColumn('VLPESOCARGABRUTA', dfBronze.VLPESOCARGABRUTA.cast(FloatType()))

        df_save = dfGold.toPandas()
        df_save.fillna('', inplace=True)
        df_save.reset_index(drop=True, inplace=True)

        batch_data = [tuple(row) for row in df_save.itertuples(index=False, name=None)]
        cursor_destino.executemany(
            'INSERT INTO [dbo].[CARGA_FATO] (IDCARGA, IDATRACACAO, ORIGEM, DESTINO, CDMERCADORIA, TIPO_OPERACAO_DA_CARGA, '
            'CARGA_GERAL_ACONDICIONAMENTO, CONTEINERESTADO, TIPO_NAVEGACAO, FLAGAUTORIZACAO, FLAGCABOTAGEM, FLAGCABOTAGEMMOVIMENTACAO, '
            'FLAGCONTEINERTAMANHO, FLAGLONGOCURSO, FLAGMCOPERACAOCARGA, FLAGOFFSHORE, FLAGTRANSPORTEVIAINTERIOIR, '
            'PERCURSO_TRANSPORTE_EM_VIAS_INTERIORES, PERCURSO_TRANSPORTE_INTERIORES, STNATUREZACARGA, STSH2, STSH4, NATUREZA_DA_CARGA, '
            'SENTIDO, TEU, QTCARGA, VLPESOCARGABRUTA) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            batch_data
        )
        cnxn_gold.commit()

    cursor_destino.close()
    cnxn_gold.close()
    cnxn_bronze.close()


def carrega_atracacao_fato():
    with conexao_db('BRONZE') as cnxn_bronze, conexao_db('GOLD') as cnxn_gold:
        cursor_destino = cnxn_gold.cursor()
        query = 'SELECT * FROM TBL_ATRACACAO'
        pdf = pd.read_sql(query, cnxn_bronze)
        dfBronze = spark.createDataFrame(pdf)
        
        dfGold = dfBronze \
            .withColumn('IDATRACACAO', dfBronze.IDATRACACAO.cast(IntegerType())) \
            .withColumn('DATA_ATRACACAO', to_timestamp('DATA_ATRACACAO', 'MM/dd/yyyy HH:mm:ss')) \
            .withColumn('DATA_CHEGADA', to_timestamp('DATA_CHEGADA', 'MM/dd/yyyy HH:mm:ss')) \
            .withColumn('DATA_DESATRACACAO', to_timestamp('DATA_DESATRACACAO', 'MM/dd/yyyy HH:mm:ss')) \
            .withColumn('DATA_INICIO_OPERACAO', to_timestamp('DATA_INICIO_OPERACAO', 'MM/dd/yyyy HH:mm:ss')) \
            .withColumn('DATA_TERMINO_OPERACAO', to_timestamp('DATA_TERMINO_OPERACAO', 'MM/dd/yyyy HH:mm:ss'))

        df_save = dfGold.toPandas()
        df_save.fillna('', inplace=True)
        df_save.reset_index(drop=True, inplace=True)

        batch_data = [tuple(row) for row in df_save.itertuples(index=False, name=None)]
        cursor_destino.executemany(
            'INSERT INTO [dbo].[ATRACACAO_FATO] (IDATRACACAO, CDTUP, IDBERCO, BERCO, PORTO_ATRACACAO, APELIDO_INSTALACAO_PORTUARIA, '
            'COMPLEXO_PORTUARIO, TIPO_DA_AUTORIDADE_PORTUARIA, DATA_ATRACACAO, DATA_CHEGADA, DATA_DESATRACACAO, DATA_INICIO_OPERACAO, '
            'DATA_TERMINO_OPERACAO, ANO, MES, TIPO_DE_OPERACAO, TIPO_DE_NAVEGACAO_DA_ATRACACAO, NACIONALIDADE_DO_ARMADOR, '
            'FLAGMCOPERACAOATRACACAO, TERMINAL, MUNICIPIO, UF, SGUF, REGIAO_GEOGRAFICA, NUM_DA_CAPITANIA, NUM_DO_IMO) VALUES '
            '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            batch_data
        )
        cnxn_gold.commit()

    cursor_destino.close()
    cnxn_gold.close()
    cnxn_bronze.close()


    
carrega_carga_fato()
carrega_atracacao_fato()
spark.stop()



