from model.conexion_db import SQLServerConnector

import os
import sys
import re
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import schedule
import time

def resource_path(relative_path):
    """ Get the absolute path to the resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temporary folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def parameters():
    global servidor, database_04, database_05,database_07, database_09, ruta_folder_glob, file_name_glob, clave_db
    file_path = resource_path(f"conexion/conexion.csv")
    df_conexion = pd.read_csv(file_path, encoding = "latin1")
    servidor = df_conexion.iloc[0]['servidor']
    database_04 = df_conexion.iloc[0]['base04']
    database_05 = df_conexion.iloc[0]['base05']
    database_07 = df_conexion.iloc[0]['base07']
    database_09 = df_conexion.iloc[0]['base09']
    ruta_folder_glob = df_conexion.iloc[0]['ruta']
    file_name_glob = df_conexion.iloc[0]['nombre_archivo']
    clave_db = df_conexion.iloc[0]['clave_sa']
parameters()

def extract_placa_regex(xml_string):
    match = re.search(r'PlacaVM="(.*?)"', xml_string)
    return match.group(1) if match else None

def extract_operador_regex(xml_string):
    match = re.search(r'NombreFigura="(.*?)"', xml_string)
    return match.group(1) if match else None

def extract_data_porte(xml_string):
    fecha_hora_origen = ''  # Inicializamos las variables
    fecha_hora_destino = ''
    try:
        if not xml_string == ' ':
            namespaces1 = {'cartaporte20': 'http://www.sat.gob.mx/CartaPorte20'}
            namespaces2 = {'cartaporte31': 'http://www.sat.gob.mx/CartaPorte31'}

            #pasarle xml
            root = ET.fromstring(xml_string)
            
            #buscar origen
            origen = root.find(".//cartaporte20:Ubicacion[@TipoUbicacion='Origen']", namespaces1)
            destino = root.find(".//cartaporte20:Ubicacion[@TipoUbicacion='Destino']", namespaces1)

            if origen is None:
                origen = root.find(".//cartaporte31:Ubicacion[@TipoUbicacion='Origen']", namespaces2)
            
            if destino is None:
                #destino
                destino = root.find(".//cartaporte31:Ubicacion[@TipoUbicacion='Destino']", namespaces2)

            if origen is not None:
                fecha_hora_origen = origen.get("FechaHoraSalidaLlegada")
                if fecha_hora_origen:
                    fecha_hora_origen =  datetime.strptime(fecha_hora_origen, "%Y-%m-%dT%H:%M:%S")
                # print("FechaHoraSalidaLlegada Origen:", fecha_hora_origen)

            if destino is not None:
                fecha_hora_destino = destino.get("FechaHoraSalidaLlegada")
                if fecha_hora_destino:
                    fecha_hora_destino =  datetime.strptime(fecha_hora_destino, "%Y-%m-%dT%H:%M:%S")
                # print("FechaHoraSalidaLlegada Destino:", fecha_hora_destino)
    except Exception as e:
        print('error data porte',e)

    return fecha_hora_origen, fecha_hora_destino

def modelo_coche(placa):
    if placa == '31BH3E':
        return 'S10'
    elif placa == '66AX5X':
        return 'F350'
    else:
        return placa

def extract_data():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_09}",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_db}")]
    sql = f'''SELECT DISTINCT 
                    P.CVE_DOC AS NUM_PEDIDO,
                    PARP.NUM_PAR,
                    C.NOMBRE AS CLIENTE,
                    V.NOMBRE AS VENDEDOR,
                    C.DIASCRED,
                    P.FECHAELAB AS FECHA_ELAB_PEDIDO,
                    P.FECHA_ENT AS FECHA_ENT_PEDIDO,
                    CONVERT(VARCHAR(10), P.FECHA_DOC, 120) AS FECHA_PEDIDO,
                    CASE 
                        WHEN F.STATUS = 'C' OR R.STATUS = 'C' OR P.STATUS = 'C' THEN 'CANCELADO'
                        WHEN F.STATUS = 'E' AND R.STATUS = 'E' AND P.STATUS = 'E' THEN 'FACTURADO' 
                        WHEN R.STATUS = 'E' AND P.STATUS = 'E' THEN 'REMISIONADO'
                        WHEN P.STATUS = 'E' THEN 'PEDIDO'
                    END AS STATUS,
                    R.FECHAELAB AS FECHA_ELAB_REMISION,
                    CONVERT(VARCHAR(10), R.FECHA_DOC, 120) AS FECHA_REMISION,
                    R.CVE_DOC AS CVE_REMISION,
                    F.FECHAELAB AS FECHA_ELAB_FACTURA,
                    CONVERT(VARCHAR(10), F.FECHA_DOC, 120) AS FECHA_FACTURA,
                    F.CVE_DOC AS CVE_FACTURA,
                    C.CLASIFIC AS CLIE_CLASIFICACION,
                    P.CVE_PEDI AS SU_PEDIDO,
                    PARP.PXS,
                    I.DESCR,
                    PARR.CANT AS CANT_REMISION,
                    PARP.CANT,
                        (SELECT TOP 1 L.LOTE 
                        FROM {database_09}.dbo.LTPD09 L
                        WHERE L.CVE_ART = PARP.CVE_ART  
                        ORDER BY L.REG_LTPD DESC) AS LOTE,
                    C.ESTADO_ENVIO,
                    C.MUNICIPIO,
                        (SELECT CAST(CP.XML_COMPLEMENTO AS VARCHAR(MAX))
                        FROM {database_09}.dbo.CARTAPORTE09 CP
                        WHERE CP.CLAVE_DOC = FT.CVE_DOC) AS CARTA_PORTE,
                    FT.FECHAELAB AS FECHA_CARTA_PORTE
                FROM {database_09}.dbo.FACTP09 P
                LEFT JOIN {database_09}.dbo.CLIE09 C ON P.CVE_CLPV = C.CLAVE
                LEFT JOIN {database_09}.dbo.PAR_FACTP09 PARP ON PARP.CVE_DOC = P.CVE_DOC
                LEFT JOIN {database_09}.dbo.VEND09 V ON P.CVE_VEND = V.CVE_VEND
                LEFT JOIN {database_09}.dbo.FACTR09 R ON R.DOC_ANT = P.CVE_DOC
                LEFT JOIN {database_09}.dbo.FACTF09 F ON F.DOC_ANT = R.CVE_DOC
                LEFT JOIN {database_09}.dbo.INVE09 I ON I.CVE_ART = PARP.CVE_ART
                LEFT JOIN {database_09}.dbo.PAR_FACTR09 PARR ON PARR.CVE_DOC = R.CVE_DOC
                LEFT JOIN {database_09}.dbo.FACTT09 FT ON FT.DOC_ANT = F.CVE_DOC
                ORDER BY P.FECHAELAB DESC;'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        
        lista_data = cursor.fetchall()
        lista_data = [list(row) for row in lista_data]
        
        df = pd.DataFrame(lista_data, columns = ['No. Pedido', 'No. Partida', 'Cliente', 'Vendedor', 'Dias de credito',
                                                 'Fecha Elab Pedido','Fecha Entrega Pedido', 'Fecha Pedido', 'STATUS','Fecha Elab Remision', 'Fecha Remision',
                                                 'CVE. Remision', 'Fecha Elab Factura', 'Fecha Factura','CVE. Factura', 'Clasificacion Clie', 'Su Pedido',
                                                 'Pendientes por Surtir', 'Descripcion','Cantidad Remisionada', 'Cantidad por Partida', 'Lote',
                                                 'Estado Envio', 'Municipio Envio','Carta Porte', 'Fecha Carta Porte'])
        df = df.fillna(' ')
        df['placa'] = df['Carta Porte'].apply(extract_placa_regex)
        df['operador'] = df['Carta Porte'].apply(extract_operador_regex)

        # print(df['Carta Porte'])
        df['Fecha Salida Origen'], df['Fecha Llegada Destino'] =  zip(*df['Carta Porte'].apply(extract_data_porte))
        df['placa'] = df['placa'].apply(modelo_coche)
        df = df.rename(columns={'placa': 'Autotransporte'})
        df.drop(columns =['Carta Porte', 'No. Partida'], inplace=True)

        ruta_archivo = resource_path(f"{ruta_folder_glob}/{file_name_glob}.csv")
        # df.to_excel(ruta_archivo, sheet_name="newsheet", index=False)
           
        df.to_csv(ruta_archivo)
    except Exception as e:
        print(e)

def job():
    print(f"Ejecutando Tarea...{datetime.now()}")
    extract_data()
    try:
        with open(resource_path(f"{ruta_folder_glob}/{file_name_glob}.csv"), "r+b") as f:
            print("✅ El archivo está libre y puede subirse.")
    except PermissionError:
        print("🚫 El archivo está en uso por otro proceso.")
    print(f"Tarea Completada...{datetime.now()}")

schedule.every(10).minutes.do(job)

print(f"Iniciando Programador de Tarea...{datetime.now()}")
extract_data()

while True:
    schedule.run_pending()
    time.sleep(1)




