from db_utils import get_db_connection

def reset_scliente_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("Eliminando tabla scliente...")
        cursor.execute("DROP TABLE IF EXISTS scliente")
        
        print("Recreando tabla scliente...")
        cursor.execute("""
        CREATE TABLE scliente (
            `codigo` VARCHAR(20),
            `nombre` VARCHAR(255),
            `direccion` VARCHAR(255),
            `cuit` VARCHAR(20),  -- Cambiado a VARCHAR
            `telefono` VARCHAR(50),
            `id_scliente` INT AUTO_INCREMENT PRIMARY KEY,
            `control_hash` VARCHAR(32) NOT NULL,
            `sync_date` DATETIME DEFAULT CURRENT_TIMESTAMP,
            `dbf_source` VARCHAR(255)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        conn.commit()
        print("Tabla scliente recreada correctamente con nuevo esquema")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    reset_scliente_table()