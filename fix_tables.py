from db_utils import get_db_connection
import mariadb

def alter_table_columns(conn: mariadb.connection, table_name: str):
    """Ajusta las columnas de texto para evitar errores de longitud"""
    cursor = conn.cursor()
    
    try:
        # Obtener estructura actual
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()
        
        # Generar ALTER TABLE para cada columna de texto
        for col in columns:
            col_name = col[0]
            col_type = col[1].upper()
            
            if 'VARCHAR' in col_type and int(col_type.split('(')[1].split(')')[0]) < 255:
                new_type = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                sql = f"ALTER TABLE `{table_name}` MODIFY `{col_name}` {new_type}"
                cursor.execute(sql)
                logger.info(f"Modificada columna {col_name} en {table_name}")
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error modificando tabla {table_name}: {str(e)}")
        return False

if __name__ == '__main__':
    tables_to_fix = ['scliente', 'cod_post', 'producto']
    conn = get_db_connection()
    
    for table in tables_to_fix:
        print(f"Procesando tabla {table}...")
        alter_table_columns(conn, table)
    
    conn.close()