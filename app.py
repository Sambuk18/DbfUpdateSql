import logging
import threading
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from threading import Thread, Lock
from flask import Flask, render_template
import json
import os

# Importaciones locales
from config import Config
from db_utils import get_db_connection, create_table_from_dbf, insert_records
from dbf_utils import get_new_records, read_dbf

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dbf_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Bloqueo para operaciones thread-safe
sync_lock = Lock()

# Estado de sincronización inicializado correctamente
sync_status = {
    'last_check': None,
    'last_positions': {file: 0 for file in Config.DBF_FILES},
    'total_processed': 0,
    'errors': [],
    'active': True
}

def sync_dbf_to_mariadb():
    """Función de sincronización mejorada"""
    conn = None
    try:
        conn = get_db_connection()
        
        for file_name in Config.DBF_FILES:
            try:
                # Verificar archivo
                file_path = Path(Config.DBF_FOLDER) / file_name
                if not file_path.exists():
                    logger.warning(f"Archivo no encontrado: {file_name}")
                    continue
                    
                # Crear tabla si no existe
                table_name = file_name.replace('.dbf', '').lower()
                if not create_table_from_dbf(conn, file_name, force_recreate=False):
                    continue
                
                # Obtener nuevos registros
                records = read_dbf(file_name)
                if not records:
                    continue
                
                # Insertar todos los registros (no solo nuevos)
                inserted, errors = insert_records(conn, table_name, records)
                
                with sync_lock:
                    sync_status['last_positions'][file_name] = len(records)
                    sync_status['total_processed'] += inserted
                    sync_status['errors'].extend(errors)
                
            except Exception as e:
                error_msg = f"Error procesando {file_name}: {str(e)}"
                logger.error(error_msg)
                with sync_lock:
                    sync_status['errors'].append(error_msg)
        
        with sync_lock:
            sync_status['last_check'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
    except Exception as e:
        error_msg = f"Error general: {str(e)}"
        logger.error(error_msg)
        with sync_lock:
            sync_status['errors'].append(error_msg)
    finally:
        if conn:
            conn.close()



def monitor_loop():
    """Bucle de monitoreo continuo con manejo seguro"""
    while sync_status['active']:
        try:
            sync_dbf_to_mariadb()
        except Exception as e:
            with sync_lock:
                sync_status['errors'].append(f"Error en bucle de monitoreo: {str(e)}")
            logger.error(f"Monitor loop error: {str(e)}")
        
        time.sleep(Config.SCAN_INTERVAL)

@app.route('/')
def status():
    """Página de estado con datos protegidos"""
    with sync_lock:
        status_data = {
            'last_check': sync_status['last_check'],
            'total_processed': sync_status['total_processed'],
            'errors': sync_status['errors'][-10:],
            'files': [
                {
                    'name': file,
                    'position': sync_status['last_positions'].get(file, 0)
                } 
                for file in Config.DBF_FILES
            ]
        }
    return render_template('status.html', status=status_data)

@app.route('/shutdown')
def shutdown():
    """Endpoint para apagar el monitor de forma segura"""
    with sync_lock:
        sync_status['active'] = False
    return "Monitor deteniéndose...", 200

def start_monitor():
    """Inicia el hilo de monitoreo con verificación"""
    if not any(t.name == "DBFMonitor" for t in threading.enumerate()):
        monitor_thread = Thread(target=monitor_loop, name="DBFMonitor")
        monitor_thread.daemon = True
        monitor_thread.start()
        logger.info("Monitor iniciado correctamente")
    else:
        logger.warning("El monitor ya está en ejecución")

if __name__ == '__main__':
    try:
        # Iniciar monitor en segundo plano
        start_monitor()
        
        # Iniciar aplicación Flask
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        with sync_lock:
            sync_status['active'] = False
        logger.info("Aplicación detenida por el usuario")
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {str(e)}")