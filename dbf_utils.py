from datetime import date, datetime
import json
import hashlib
from pathlib import Path
from config import Config
import dbfread
import logging
from typing import Dict, List, Tuple, Any, Union

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_hash(record: Dict[str, Any]) -> str:
    """
    Calcula hash MD5 de un registro, manejando tipos especiales como fechas y valores nulos
    
    Args:
        record: Diccionario con los datos del registro DBF
        
    Returns:
        str: Hash MD5 del registro serializado
    """
    def json_serial(obj: Any) -> Any:
        """
        Serializador personalizado para objetos no serializables por defecto
        
        Args:
            obj: Objeto a serializar
            
        Returns:
            Versión serializable del objeto
            
        Raises:
            TypeError: Si el tipo no puede ser serializado
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif obj is None:
            return ""  # Convertir None a string vacío
        raise TypeError(f"Tipo no serializable: {type(obj)}")
    
    try:
        record_str = json.dumps(record, default=json_serial, sort_keys=True).encode('utf-8')
        return hashlib.md5(record_str).hexdigest()
    except Exception as e:
        logger.error(f"Error calculando hash: {str(e)}")
        return ""


def read_dbf(file_name: str) -> List[Dict[str, Any]]:
    """
    Lee un archivo DBF con múltiples intentos y manejo de errores mejorado
    """
    file_path = Path(Config.DBF_FOLDER) / file_name
    
    if not file_path.exists():
        logger.error(f"Archivo DBF no encontrado: {file_path}")
        return []
    
    # Intentar con diferentes configuraciones
    encodings = ['latin-1', 'utf-8', 'cp1252', 'ascii']
    ignore_missing_memo = [True, False]
    char_decode_options = ['replace', 'ignore', 'strict']
    
    for encoding in encodings:
        for ignore_memo in ignore_missing_memo:
            for decode_option in char_decode_options:
                try:
                    table = dbfread.DBF(
                        file_path,
                        encoding=encoding,
                        ignore_missing_memofile=ignore_memo,
                        char_decode_errors=decode_option
                    )
                    records = list(table)
                    if records:
                        logger.info(f"Archivo {file_name} leído con encoding {encoding}, ignore_memo={ignore_memo}")
                        return records
                except Exception as e:
                    logger.debug(f"Intento fallido con encoding {encoding}: {str(e)}")
                    continue
    
    logger.error(f"No se pudo leer el archivo DBF con ninguna configuración: {file_name}")
    return []



def get_new_records(file_name: str, last_positions: Dict[str, int]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Obtiene nuevos registros desde la última posición conocida con verificación de consistencia
    
    Args:
        file_name: Nombre del archivo DBF
        last_positions: Diccionario con las últimas posiciones conocidas
        
    Returns:
        Tupla con (nuevos_registros, nueva_posición)
    """
    try:
        records = read_dbf(file_name)
        if not records:
            return [], 0
        
        last_position = last_positions.get(file_name, 0)
        
        # Verificar consistencia de posición
        if last_position > len(records):
            logger.warning(f"Posición inválida para {file_name}. Reiniciando contador.")
            last_position = 0
        
        new_records = records[last_position:]
        return new_records, len(records)
        
    except Exception as e:
        logger.error(f"Error obteniendo nuevos registros de {file_name}: {str(e)}")
        return [], 0

def get_dbf_structure(file_name: str) -> Dict[str, str]:
    """
    Obtiene la estructura de un archivo DBF (nombres y tipos de campos)
    
    Args:
        file_name: Nombre del archivo DBF
        
    Returns:
        Diccionario con {nombre_campo: tipo_dato}
    """
    try:
        file_path = Path(Config.DBF_FOLDER) / file_name
        if not file_path.exists():
            return {}
            
        table = dbfread.DBF(file_path, load_records=False)
        return {field.name: field.type for field in table.fields}
    except Exception as e:
        logger.error(f"Error obteniendo estructura de {file_name}: {str(e)}")
        return {}