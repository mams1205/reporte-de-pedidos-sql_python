from model.conexion_db import SQLServerConnector
from tkinter import messagebox
import pandas as pd
import numpy as np
from datetime import datetime
import pyodbc as podbc

import os
import sys

def resource_path(relative_path):
    """ Get the absolute path to the resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temporary folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def parameters():
    global servidor, database_04, database_05,database_07, database_09, clave_sa
    file_path = resource_path(f"conexion/conexion.csv")
    df_conexion = pd.read_csv(file_path)
    servidor = df_conexion.iloc[0]['servidor']
    database_04 = df_conexion.iloc[0]['base04']
    database_05 = df_conexion.iloc[0]['base05']
    database_07 = df_conexion.iloc[0]['base07']
    database_09 = df_conexion.iloc[0]['base09']
    clave_sa = df_conexion.iloc[0]['clave_sa']
parameters()

def query_login(usuario, password):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicAPG",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}")]

    # Example query
    sql = f'''SELECT [U_Nom],
                     [U_Pas]
               FROM 
                    [SlicAPG].[dbo].[USUARIOS]
                WHERE
                    [U_Nom] = ? AND
                    [U_Pas] = ? '''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (usuario, password))
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if result:
            return True
        else:
            return False
    except Exception as e:
        titulo = "Error query_login"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def muestra_facturas(empresa,cliente, fecha_inicial, fecha_final):
    if int(empresa) == 7:
        data_base = database_07
        clave_db = '07'
    elif int(empresa) == 9:
        data_base = database_09
        clave_db = '09'
    else:
        data_base = database_04
        clave_db = '04'
   
# Define multiple instances for different databases
    
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicAPG",
            username="sa",          # Siempre el usuario 'sa'
            password=f"{clave_sa}"),
        
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{data_base}",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}")
        ]
    lista_facturas = []
    # Example query
    sql= f'''SELECT 
                     M.CVE_CLIE,
                    C.NOMBRE,
                    M.NO_FACTURA,
                    CAST(M.FECHA_APLI AS DATE) AS FECHA_APLI,
                    CAST(M.FECHA_VENC AS DATE) AS FECHA_VENC, 
                    ROUND(M.IMPMON_EXT,2) AS IMPORTE,
                    ROUND(SUM(ISNULL(D.IMPMON_EXT, 0)) - M.IMPMON_EXT,2) AS SALDO_FINAL,
                    ISNULL(TC.CVE_MONED, TC.SIMBOLO) AS MONEDA,
                    DATEDIFF(DAY,GETDATE(), M.FECHA_VENC) AS DiferenciaEnDias,
                    CASE 
						WHEN SD.num_factura IS NOT NULL THEN 'SI'
						ELSE 'NO'  
					END AS PDF
                FROM {data_base}.dbo.CUEN_M{clave_db} M
                LEFT JOIN {data_base}.dbo.MONED{clave_db} TC ON M.NUM_MONED = TC.NUM_MONED
                LEFT JOIN {data_base}.dbo.CONC{clave_db} O ON M.NUM_CPTO = O.NUM_CPTO
                LEFT JOIN {data_base}.dbo.CLIE{clave_db} C ON M.CVE_CLIE = C.CLAVE
                LEFT JOIN {data_base}.dbo.CUEN_DET{clave_db} D ON M.CVE_CLIE = D.CVE_CLIE AND M.NO_FACTURA = D.NO_FACTURA
                LEFT JOIN SlicAPG.dbo.estados_cuenta_det SD ON LTRIM(RTRIM(M.NO_FACTURA)) COLLATE SQL_Latin1_General_CP1_CI_AS = LTRIM(RTRIM(SD.num_factura)) COLLATE SQL_Latin1_General_CP1_CI_AS
                WHERE 
                    (LTRIM(RTRIM(C.CLAVE)) = ? OR ? IS NULL OR ? = '') 
                    AND M.FECHA_VENC <= ? 
                    AND M.FECHA_VENC >= ?
                GROUP BY M.FECHA_APLI, M.FECHA_VENC, O.DESCR, C.NOMBRE, M.CVE_CLIE, M.NO_FACTURA, M.IMPMON_EXT, SD.num_factura, TC.CVE_MONED, TC.SIMBOLO
                HAVING ROUND(SUM(ISNULL(D.IMPMON_EXT, 0)),2) - M.IMPMON_EXT < -0.01
                ORDER BY FECHA_VENC DESC'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (cliente, cliente, cliente, fecha_final, fecha_inicial))
        lista_facturas = cursor.fetchall()
        lista_facturas = [list(row) for row in lista_facturas]

        messagebox.showinfo('Busqueda completada', f'Se encontraron {len(lista_facturas)} coincidencias')
       
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    return lista_facturas

def insert_data_factura(name_cliente, cve_cliente, tot_facturas, ruta_pdf, lista_detalle, empresa):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicAPG",
            username="sa",          # Siempre el usuario 'sa'
            password=f"{clave_sa}")
    ]
    
    # Get the current date
    fecha = datetime.today()

    # Define SQL queries
    sql_h = '''INSERT INTO [SlicAPG].[dbo].[estados_cuenta_h] 
               (nombre,cve_cliente, tot_facturas, fecha_creacion, status_pdf, ruta_pdf,empresa)
               VALUES(?,?, ?, ?, 'C', ?, ?)'''

    sql_select_id = '''SELECT MAX(id)
                       FROM [SlicAPG].[dbo].[estados_cuenta_h]'''

    sql_ins_det = '''INSERT INTO [SlicAPG].[dbo].[estados_cuenta_det]
                      (id_ec, num_factura, importe, fecha_vencimiento, moneda, id_detalle_ec)
                      VALUES(?, ?, ?, ?, ?, ?)'''

    try:
        # Establecer la conexión y crear el cursor
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        # Insertar en el encabezado
        cursor.execute(sql_h, name_cliente, int(cve_cliente), tot_facturas, fecha, ruta_pdf, empresa)
        connection.commit()

        # Obtener el id_max después de insertar en el encabezado
        cursor.execute(sql_select_id)
        id_max = cursor.fetchone()[0]

        # Insertar los detalles uno por uno usando un bucle for
        for row in lista_detalle:
            # Asegurarse de que 'row' sea una lista (si no lo es, convertirlo)
            row = list(row)
            # Ejecutar la inserción de cada fila de detalle
            cursor.execute(sql_ins_det, (id_max, *row))
            connection.commit()  # Commit después de cada inserción (si prefieres hacerlo así)

        # Cerrar la conexión
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error en query insert_data_factura"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return id_max

def muestra_pdf():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicAPG",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}"
        )]
    lista_pdf= []
    # Example query
    sql= f'''SELECT
                id,
                empresa,
                cve_cliente,
                nombre,
                tot_facturas,
                CAST([fecha_creacion] AS DATE) AS fecha_creacion,
                status_pdf,
                CAST([fecha_envio] AS DATE) AS fecha_envio,
                ruta_pdf
            FROM [SlicAPG].[dbo].[estados_cuenta_h]
            ORDER BY fecha_creacion ASC;
            '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_pdf = cursor.fetchall()
        lista_pdf = [list(row) for row in lista_pdf]
       
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_pdf

def consulta_detalle(id_value):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicAPG",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}")]
    
    lista_detalle= []
    # Example query
    sql= f'''SELECT
                id_ec,
                id_detalle_ec,
                num_factura,
                importe,
                CAST([fecha_vencimiento] AS DATE) AS fecha_vencimiento
            FROM [SlicAPG].[dbo].[estados_cuenta_det]
            WHERE id_ec = ?
            ORDER BY id_detalle_ec DESC;
            '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, id_value)
        lista_detalle = cursor.fetchall()
        lista_detalle = [[row[0], row[1], row[2], f"${row[3]:,.2f}", row[4]] 
                         for row in lista_detalle
                         ]
       
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_detalle
    
def muestra_pdf_cliente(cliente_name):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicAPG",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}")]
    lista_pdf= []
    # Example query
    sql= f'''SELECT
                id,
                empresa,
                cve_cliente,
                nombre,
                tot_facturas,
                CAST([fecha_creacion] AS DATE) AS fecha_creacion,
                status_pdf,
                CAST([fecha_envio] AS DATE) AS fecha_envio,
                ruta_pdf
            FROM [SlicAPG].[dbo].[estados_cuenta_h]
            WHERE nombre like '%{cliente_name}%'
            ORDER BY id DESC;
            '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_pdf = cursor.fetchall()
        lista_pdf = [list(row) for row in lista_pdf]
       
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_pdf

def data_mail(cliente_cve, empresa):
    if empresa == 7:
        data_base = database_07
        clave_db = '07'
    elif empresa == 9:
        data_base = database_09
        clave_db = '09'
    else:
        data_base = database_04
        clave_db = '04'

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicAPG",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}"),

        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{data_base}",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}"
        )]
    
    # Example query
    sql_email = f'''SELECT DISTINCT(b.EMAIL) AS EMAIL, ISNULL(b.TIPOCONTAC, 'X')
                    FROM SlicAPG.dbo.estados_cuenta_h a 
                    LEFT JOIN {data_base}.dbo.CONTAC{clave_db} b ON LTRIM(RTRIM(b.CVE_CLIE)) COLLATE SQL_Latin1_General_CP1_CI_AS = a.cve_cliente
                    WHERE a.cve_cliente = ? AND b.EMAIL is not null
                    GROUP BY b.EMAIL, b.TIPOCONTAC;'''
    # sql_email= f'''SELECT b.EMAILPRED
    #         FROM estados_cuenta_h a 
    #         LEFT JOIN {data_base}.dbo.CLIE{clave_db} b 
    #             ON LTRIM(RTRIM(b.CLAVE)) COLLATE SQL_Latin1_General_CP1_CI_AS = a.cve_cliente
    #         WHERE a.cve_cliente = ?;
    #         '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_email, cliente_cve)

        receiver_mail = cursor.fetchall()
        receiver_mail = [list(row) for row in receiver_mail]
        receiver_mail = pd.DataFrame(receiver_mail, columns = ['mail', 'status'])

        if (receiver_mail['status'] == 'C').any():
            receiver_mail_n =  receiver_mail[receiver_mail['status'] == 'C']
            lista_mail = receiver_mail_n['mail'].tolist()
            
            # nuevo_correo = "creditoycobranza@apgquimica.com.mx"
            # lista_mail.append(nuevo_correo)
        else:
            receiver_mail_n = receiver_mail
            lista_mail = receiver_mail_n['mail'].tolist()
            # nuevo_correo = "creditoycobranza@apgquimica.com.mx"
            # lista_mail.append(nuevo_correo)


        # receiver_mail = cursor.fetchone()[0]
        # receiver_mail = receiver_mail.split(';')
        # receiver_mail = [elemento.strip() for elemento in receiver_mail]
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return lista_mail

def update_status_mail(id_h):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicAPG",
        username="sa",          # Siempre el usuario 'sa'
        password=f"{clave_sa}")]
    
    fecha = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # Example query
    sql= f'''UPDATE SlicAPG.dbo.estados_cuenta_h
             SET status_pdf = 'E',
                 fecha_envio = ?
             WHERE id = ?'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql, (fecha, id_h))
        
        connection.commit() 

       
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

