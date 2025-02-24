import pandas as pd
import os
import pyodbc

def importa_arq(arquivo):
    caminho = os.path.join('C:\\ANTAQ\\ARQUIVOS', arquivo)
    try:
        dados = pd.read_csv(caminho, sep=';', low_memory=False)
        dados.fillna('', inplace=True)
        return dados
    except Exception as e:
        print(f'Erro ao importar {arquivo}: {e}')
        return None

def conexao_db(banco):
    host = '127.0.0.1'
    login = 'tass'
    pwd = 'Tulio2023'
    try:
        return pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={banco};UID={login};PWD={pwd}'
        )
    except pyodbc.InterfaceError as e:
        print(f'Erro de conexão: {e}')
        return None

def import_tbl_carga():
    con = conexao_db('BRONZE')
    if con is None:
        return
    
    dados = importa_arq('Carga.txt')
    if dados is None:
        return
    
    colunas = [
        'IDCarga', 'IDAtracacao', 'Origem', 'Destino', 'CDMercadoria',
        'Tipo Operação da Carga', 'Carga Geral Acondicionamento', 'ConteinerEstado',
        'Tipo Navegação', 'FlagAutorizacao', 'FlagCabotagem', 'FlagCabotagemMovimentacao',
        'FlagConteinerTamanho', 'FlagLongoCurso', 'FlagMCOperacaoCarga', 'FlagOffshore',
        'FlagTransporteViaInterioir', 'Percurso Transporte em vias Interiores',
        'Percurso Transporte Interiores', 'STNaturezaCarga', 'STSH2', 'STSH4',
        'Natureza da Carga', 'Sentido', 'TEU', 'QTCarga', 'VLPesoCargaBruta'
    ]
    
    query = '''INSERT INTO [dbo].[TBL_CARGA] ({}) VALUES ({})'''.format(
        ','.join(f'[{col}]' for col in colunas), ','.join('?' * len(colunas))
    )
    
    valores = [tuple(linha.get(col, '') for col in colunas) for _, linha in dados.iterrows()]
    
    try:
        with con.cursor() as cursor:
            cursor.executemany(query, valores)
            con.commit()
    except Exception as e:
        print(f'Erro ao inserir dados na TBL_CARGA: {e}')
    finally:
        con.close()

def import_tbl_atracao():
    con = conexao_db('BRONZE')
    if con is None:
        return
    
    dados = importa_arq('Atracacao.txt')
    if dados is None:
        return
    
    colunas = [
        'IDAtracacao', 'CDTUP', 'IDBerco', 'Berço', 'Porto Atracação',
        'Apelido Instalação Portuária', 'Complexo Portuário', 'Tipo da Autoridade Portuária',
        'Data Atracação', 'Data Chegada', 'Data Desatracação', 'Data Início Operação',
        'Data Término Operação', 'Ano', 'Mes', 'Tipo de Operação',
        'Tipo de Navegação da Atracação', 'Nacionalidade do Armador',
        'FlagMCOperacaoAtracacao', 'Terminal', 'Município', 'UF', 'SGUF',
        'Região Geográfica', 'Nº da Capitania', 'Nº do IMO'
    ]
    
    query = '''INSERT INTO [dbo].[TBL_ATRACACAO] ({}) VALUES ({})'''.format(
        ','.join(f'[{col}]' for col in colunas), ','.join('?' * len(colunas))
    )
    
    valores = [tuple(linha.get(col, '') for col in colunas) for _, linha in dados.iterrows()]
    
    try:
        with con.cursor() as cursor:
            cursor.executemany(query, valores)
            con.commit()
    except Exception as e:
        print(f'Erro ao inserir dados na TBL_ATRACACAO: {e}')
    finally:
        con.close()

import_tbl_carga()
import_tbl_atracao()
