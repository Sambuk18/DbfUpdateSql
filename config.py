import os
from pathlib import Path

# Configuración básica
class Config:
    DBF_FOLDER = "/var/www/proyectos/DbfUpdateSql/DBF"  # Cambiar por tu ruta real
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'usuariosyspas',
        'password': 'migu3lit0chhavela',
        'database': 'recibos_db',
        'port': 3306
    }
    
    # Archivos DBF a monitorear
    DBF_FILES = [
        'con_gast.dbf', 'ctacte.dbf', 'recibos.dbf', 'sfac_tur.dbf',
        'scliente.dbf', 'omega.dbf', 'printer.dbf', 'cod_post.dbf',
        'planinte.dbf', 'producto.dbf', 'cuentas.dbf', 'cajas.dbf', 'recimig.dbf'
    ]
    
    # Tiempo entre verificaciones (segundos)
    SCAN_INTERVAL = 60