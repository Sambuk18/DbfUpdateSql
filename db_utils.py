import mariadb
import time
from datetime import date, datetime
from typing import List, Dict, Any, Tuple
import logging
from config import Config
import traceback
from dbf_utils import read_dbf, calculate_hash

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection(max_retries: int = 3, retry_delay: int = 2) -> mariadb.connection:
    """Establece conexión con MariaDB con reintentos"""
    attempt = 0
    last_error = None
    
    while attempt < max_retries:
        try:
            conn = mariadb.connect(**Config.DB_CONFIG)
            logger.info("Conexión a MariaDB establecida correctamente")
            return conn
        except mariadb.Error as e:
            last_error = e
            attempt += 1
            logger.warning(f"Intento {attempt}/{max_retries} fallido: {str(e)}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    
    logger.error("No se pudo establecer conexión con MariaDB")
    raise RuntimeError(f"No se pudo conectar a MariaDB después de {max_retries} intentos: {str(last_error)}")

def _determine_column_type(field_name: str, value: Any, sample_records: List[Dict[str, Any]]) -> str:
    """Determina el tipo de columna SQL con análisis de longitud"""
    field_lower = field_name.lower()
    
    # Campos especiales
    if field_lower in ['cuit', 'cuil', 'dni', 'codigo']:
        return "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    
    # Análisis de longitud para campos de texto
    if isinstance(value, str):
        try:
            max_len = max(len(str(rec.get(field_name, ''))) for rec in sample_records)
            max_len = max(max_len, len(str(value)))
            padding = 10
            
            if max_len + padding <= 255:
                return f"VARCHAR({max_len + padding}) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            return "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        except Exception as e:
            logger.warning(f"Error calculando longitud para {field_name}: {str(e)}")
            return "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    
    elif isinstance(value, int):
        return "BIGINT" if abs(value) > 2147483647 else "INT"
    elif isinstance(value, float):
        return "DECIMAL(15,2)"
    elif isinstance(value, (datetime, date)):
        return "DATETIME"
    elif isinstance(value, bool):
        return "BOOLEAN"
    return "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"

def create_table_from_dbf(conn: mariadb.connection, file_name: str, force_recreate: bool = False) -> bool:
    """Crea tabla solo si no existe o si se fuerza recreación"""
    cursor = conn.cursor()
    table_name = file_name.replace('.dbf', '').lower()
    
    try:
        # Verificar si tabla ya existe
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            if force_recreate:
                logger.info(f"Eliminando tabla existente {table_name} para recreación")
                cursor.execute(f"DROP TABLE `{table_name}`")
                conn.commit()
            else:
                logger.info(f"Tabla {table_name} ya existe - continuando")
                return True  # Tabla existe, no es un error
        
        # Leer registros para determinar estructura
        all_records = read_dbf(file_name)
        if not all_records:
            logger.error(f"No se pudieron leer registros de {file_name}")
            return False
        
        # Generar SQL para crear tabla
        columns = []
        for field in all_records[0].keys():
            col_type = _determine_column_type(field, all_records[0], all_records)
            columns.append(f"`{field.lower()}` {col_type}")
        
        # Campos adicionales
        columns.append(f"`id_{table_name}` INT AUTO_INCREMENT PRIMARY KEY")
        columns.append("`control_hash` VARCHAR(32) NOT NULL UNIQUE")
        columns.append("`sync_date` DATETIME DEFAULT CURRENT_TIMESTAMP")
        columns.append("`dbf_source` VARCHAR(255) DEFAULT NULL")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  " + ",\n  ".join(columns) + "\n)"
        
        for attempt in range(3):
            try:
                cursor.execute(create_sql)
                conn.commit()
                logger.info(f"Tabla {table_name} creada exitosamente")
                return True
            except mariadb.Error as e:
                if attempt == 2:
                    logger.error(f"Error creando tabla {table_name}: {str(e)}")
                    conn.rollback()
                    return False
                time.sleep(1)
                
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}\n{traceback.format_exc()}")
        return False

def insert_records(conn: mariadb.connection, table_name: str, records: List[Dict[str, Any]], 
                 batch_size: int = 100) -> Tuple[int, int, List[str]]:
    """
    Inserta registros usando control_hash para evitar duplicados
    Devuelve: (insertados, actualizados, errores)
    """
    if not records:
        return 0, 0, []

    cursor = conn.cursor()
    errors = []
    inserted = 0
    updated = 0
    fields = list(records[0].keys())

    try:
        # Verificar estructura de la tabla
        cursor.execute(f"DESCRIBE `{table_name}`")
        table_structure = {row[0]: row[1] for row in cursor.fetchall()}

        # Preparar SQL para verificar existencia por hash
        check_sql = f"SELECT 1 FROM `{table_name}` WHERE control_hash = %s LIMIT 1"
        
        # Preparar SQL de inserción
        columns = [f"`{field.lower()}`" for field in fields 
                 if field.lower() in table_structure]
        columns.append("`control_hash`")
        
        placeholders = ["%s"] * len(columns)
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        # Preparar SQL de actualización
        set_clause = ", ".join([f"`{col}`=%s" for col in columns if col != '`control_hash`'])
        update_sql = f"""
            UPDATE `{table_name}` 
            SET {set_clause}, sync_date=CURRENT_TIMESTAMP 
            WHERE control_hash=%s
        """

        # Procesar registros
        for record in records:
            try:
                # Preparar valores
                values = []
                for field in fields:
                    field_lower = field.lower()
                    if field_lower in table_structure:
                        value = record.get(field)
                        # Manejo de tipos como antes...
                        values.append(value)
                
                record_hash = calculate_hash(record)
                
                # Verificar si existe
                cursor.execute(check_sql, (record_hash,))
                exists = cursor.fetchone()
                
                if exists:
                    # Actualizar registro existente
                    update_values = values + [record_hash]
                    cursor.execute(update_sql, update_values)
                    updated += 1
                else:
                    # Insertar nuevo registro
                    insert_values = values + [record_hash]
                    cursor.execute(insert_sql, insert_values)
                    inserted += 1
                
            except Exception as e:
                errors.append(f"Error procesando registro: {str(e)}")
                conn.rollback()
                continue
        
        conn.commit()
        logger.info(f"Operación completada: {inserted} nuevos, {updated} actualizados en {table_name}")

    except Exception as e:
        conn.rollback()
        errors.append(f"Error general: {str(e)}")
        logger.error(f"Error crítico: {str(e)}\n{traceback.format_exc()}")

    return inserted, updated, errors

def backup_table(conn: mariadb.connection, table_name: str) -> bool:
    """Crea backup de una tabla"""
    backup_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{backup_name}` LIKE `{table_name}`")
        cursor.execute(f"INSERT INTO `{backup_name}` SELECT * FROM `{table_name}`")
        conn.commit()
        return True
    except mariadb.Error as e:
        conn.rollback()
        logger.error(f"Error creando backup: {str(e)}")
        return False