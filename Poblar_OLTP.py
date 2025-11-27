import psycopg2
import os

# --- 1. Configuración de la Base de Datos ---
DB_CONFIG = {
    "host": "localhost",
    "database": "techzone_OLTP",
    "user": "admin",
    "password": "admin123",
    "port": "5432"
}

# --- 2. Configuración de Archivos y Tablas ---
# Define la carpeta donde se encuentran todos tus CSVs
CSV_DIRECTORY = "/home/santino-giampietro/Descargas/techzone_arg_final"

# Crea un diccionario para mapear {nombre_del_archivo_csv: nombre_de_la_tabla_destino}
# Asegúrate de que los nombres de las columnas del CSV coincidan con los de la tabla.
# --- 2. Configuración de Archivos y Tablas (ORDEN CORREGIDO) ---
ARCHIVOS_A_IMPORTAR = {
    # NIVEL 1: Tablas primarias (sin FKs a otras tablas)
    "categoria.csv": "categoria",
    "cliente.csv": "cliente",
    "proveedor.csv": "proveedor",
    "empleado.csv": "empleado",

    # NIVEL 2: Tablas que referencian a Nivel 1
    "producto.csv": "producto",           # Necesita categoria, proveedor
    "pedido.csv": "pedido",               # Necesita cliente, empleado
    "orden_compra.csv": "orden_compra",   # Necesita proveedor

    # NIVEL 3: Tablas que referencian a Nivel 2
    "envio.csv": "envio",
    "factura.csv": "factura",
    "detalle_pedido.csv": "detalle_pedido",       # Necesita pedido, producto
    "detalle_orden_compra.csv": "detalle_orden_compra", # Necesita orden_compra, producto

    # NIVEL 4: Tablas que referencian a Nivel 3
    "pago.csv": "pago",                   # Necesita factura
}

# Opciones de importación (común para CSVs)
DELIMITER = ','
HEADER = 'TRUE' # 'TRUE' si la primera fila es encabezado, 'FALSE' si no lo es

# --- 3. Función de Importación ---

def importar_csv_con_copy(csv_path, table_name, delimiter=DELIMITER, header=HEADER):
    """Importa un archivo CSV a PostgreSQL usando el comando COPY."""
    conn = None
    try:
        # 1. Conectar a la base de datos
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 2. Abrir el archivo CSV
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Construir el comando SQL COPY
            copy_sql = f"""
                COPY {table_name} 
                FROM STDIN 
                WITH (FORMAT CSV, HEADER {header}, DELIMITER '{delimiter}')
            """
            
            # 3. Ejecutar la copia de datos
            cursor.copy_expert(sql=copy_sql, file=f)
        
        # 4. Confirmar la transacción
        conn.commit()
        print(f"✅ Éxito: '{os.path.basename(csv_path)}' -> '{table_name}'.")

    except psycopg2.Error as e:
        print(f"❌ Error de PostgreSQL al importar {table_name}: {e}")
        if conn:
            conn.rollback() # Deshacer la transacción si falla
    except FileNotFoundError:
        print(f"❌ Error: Archivo no encontrado en la ruta: {csv_path}")
    except Exception as e:
        print(f"❌ Error inesperado con {table_name}: {e}")
    finally:
        # 5. Cerrar la conexión
        if conn:
            conn.close()

# --- 4. Ejecución Principal ---

if __name__ == "__main__":
    print("--- 🏁 Iniciando importación de múltiples CSVs a PostgreSQL ---")
    
    for csv_file, table_name in ARCHIVOS_A_IMPORTAR.items():
        full_csv_path = os.path.join(CSV_DIRECTORY, csv_file)
        
        # Solo intentar importar si el archivo existe
        if os.path.exists(full_csv_path):
            importar_csv_con_copy(full_csv_path, table_name)
        else:
            print(f"⚠️ Advertencia: Archivo '{csv_file}' no encontrado en el directorio especificado.")
            
    print("--- ✅ Proceso de importación finalizado. ---")