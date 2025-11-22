import pandas as pd
import numpy as np
from sqlalchemy import create_engine

class ETLProcessor:
    
    def __init__(self, source_conn_string: str, target_conn_string: str):
        self.source_engine = create_engine(source_conn_string)
        self.target_engine = create_engine(target_conn_string)
    
    def extract_data(self):
        
        queries = {
            'clientes': "SELECT * FROM cliente",
            'productos': "SELECT * FROM producto", 
            'categorias': "SELECT * FROM categoria",
            'proveedores': "SELECT * FROM proveedor",
            'empleados': "SELECT * FROM empleado",
            'pedidos': "SELECT * FROM pedido",
            'detalle_pedido': "SELECT * FROM detalle_pedido",
            'detalle_orden_compra': "SELECT * FROM detalle_orden_compra",
            'envios': "SELECT * FROM envio"  
        }
        
        data = {}
        for name, query in queries.items():
            try:
                data[name] = pd.read_sql(query, self.source_engine)
            except Exception as e:
                data[name] = pd.DataFrame()
        return data
    
    def transform_dim_tiempo(self, df_pedidos, df_envios):
        """
        Crea dimensión tiempo unificando fechas de pedidos y envíos.
        """
        fechas = []
        
        # Recolectar fechas de pedidos
        if 'fecha' in df_pedidos.columns:
            fechas.extend(pd.to_datetime(df_pedidos['fecha']).dropna())
            
        # Recolectar fechas de envíos (entrega y salida)
        if not df_envios.empty:
            if 'fecha_entrega' in df_envios.columns:
                fechas.extend(pd.to_datetime(df_envios['fecha_entrega']).dropna())
            if 'fecha_envio' in df_envios.columns:
                fechas.extend(pd.to_datetime(df_envios['fecha_envio']).dropna())

        # Normalizar (quitar horas) y ordenar únicos
        fechas = pd.Series(fechas).dt.normalize().unique()
        fechas = np.sort(fechas)
        
        if len(fechas) == 0:
            fechas = [pd.Timestamp('2023-01-01')] # Fallback

        df_fechas = pd.DataFrame({'fecha_date': fechas})
        df_fechas['id_tiempo'] = range(1, len(df_fechas) + 1)
        
        # Atributos
        df_fechas['dia'] = df_fechas['fecha_date'].dt.day
        df_fechas['mes'] = df_fechas['fecha_date'].dt.month
        df_fechas['año'] = df_fechas['fecha_date'].dt.year
        df_fechas['trimestre'] = df_fechas['fecha_date'].dt.quarter
        df_fechas['semestre'] = np.where(df_fechas['fecha_date'].dt.month <= 6, 1, 2)
        
        # Textos
        meses_es = {1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril', 5:'Mayo', 6:'Junio',
                    7:'Julio', 8:'Agosto', 9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'}
        dias_es = {0:'Lunes', 1:'Martes', 2:'Miércoles', 3:'Jueves', 4:'Viernes', 5:'Sábado', 6:'Domingo'}
        
        df_fechas['nombre_mes'] = df_fechas['fecha_date'].dt.month.map(meses_es)
        df_fechas['nombre_dia_semana'] = df_fechas['fecha_date'].dt.dayofweek.map(dias_es)
        df_fechas['es_fin_semana'] = df_fechas['fecha_date'].dt.dayofweek.isin([5, 6])
        df_fechas['semana_anio'] = df_fechas['fecha_date'].dt.isocalendar().week.astype(int)
        
        return df_fechas

    def transform_dim_ubicacion(self, df_clientes):
        # Lógica simplificada para extraer ubicación de la dirección
        ubicaciones = df_clientes[['direccion']].drop_duplicates().copy()
        
        # Aquí asumimos que todos son de una ubicación genérica si no se puede parsear
        # Esto evita perder clientes por falta de datos geográficos exactos
        ubicaciones['ciudad'] = 'Desconocida'
        ubicaciones['region'] = 'Desconocida'
        ubicaciones['pais'] = 'Argentina'
        
        ubicaciones = ubicaciones[['ciudad', 'region', 'pais']].drop_duplicates().reset_index(drop=True)
        ubicaciones['id_ubicacion'] = range(1, len(ubicaciones) + 1)
        return ubicaciones

    def transform_dim_producto(self, df_productos, df_categorias, df_proveedores):
        df = df_productos.copy()
        
        # LEFT JOIN es crucial: si un producto no tiene categoría asignada, no lo perdemos
        if not df_categorias.empty:
            df = df.merge(df_categorias, on='id_categoria', how='left', suffixes=('', '_cat'))
            df.rename(columns={'nombre_cat': 'nombre_categoria'}, inplace=True)
            
        if not df_proveedores.empty:
            df = df.merge(df_proveedores, on='id_proveedor', how='left', suffixes=('', '_prov'))
            df.rename(columns={'nombre_prov': 'nombre_proveedor'}, inplace=True)
            
        df['nombre_categoria'] = df['nombre_categoria'].fillna('Sin Categoría')
        df['nombre_proveedor'] = df['nombre_proveedor'].fillna('Sin Proveedor')
        
        return df[['id_producto', 'nombre', 'precio', 'stock', 'id_categoria', 
                   'nombre_categoria', 'id_proveedor', 'nombre_proveedor']]

    def calculate_average_cost(self, df_detalle_compra):
        if df_detalle_compra.empty:
            return pd.DataFrame(columns=['id_producto', 'costo_promedio'])
        avg_cost = df_detalle_compra.groupby('id_producto')['costo_unitario'].mean().reset_index()
        avg_cost.rename(columns={'costo_unitario': 'costo_promedio'}, inplace=True)
        return avg_cost

    def transform_hechos_ventas(self, dfs, dim_tiempo, dim_ubicacion):
        """
        Construye la tabla de hechos uniendo Pedidos, Detalles y Envíos.
        """
        print("   > Uniendo Pedidos con Detalles...")
        # 1. Base: Detalle Pedido (Granularidad fina) + Pedido (Cabecera)
        df_base = dfs['detalle_pedido'].merge(dfs['pedidos'], on='id_pedido', how='inner')
        
        print("   > Uniendo Información de Envíos...")
        # 2. Unir Envíos (CORRECCIÓN: LEFT JOIN)
        # Usamos LEFT JOIN porque si el pedido es nuevo, NO existe en la tabla envios aun.
        # Si usáramos inner, perderíamos las ventas recientes.
        if not dfs['envios'].empty:
            df_base = df_base.merge(dfs['envios'], on='id_pedido', how='left', suffixes=('', '_envio'))
        else:
            # Crear columnas vacías si la tabla envios no trajo nada
            df_base['fecha_entrega'] = pd.NaT
            df_base['estado_envio'] = None 

        # 3. Calcular Costos y Margen
        df_costos = self.calculate_average_cost(dfs['detalle_orden_compra'])
        df_base = df_base.merge(df_costos, on='id_producto', how='left')
        df_base['costo_promedio'] = df_base['costo_promedio'].fillna(df_base['precio_unitario'] * 0.7)

        # 4. Resolver Dimensiones
        # Tiempo
        df_base['fecha_norm'] = pd.to_datetime(df_base['fecha']).dt.normalize()
        df_base = df_base.merge(dim_tiempo[['fecha_date', 'id_tiempo']], 
                                left_on='fecha_norm', right_on='fecha_date', how='left')
        
        # Ubicación (Default id 1 para asegurar carga)
        df_base['id_ubicacion'] = 1 
        
        # 5. Cálculos Numéricos
        df_base['total_venta'] = df_base['cantidad'] * df_base['precio_unitario']
        df_base['costo_total'] = df_base['cantidad'] * df_base['costo_promedio']
        df_base['margen_ganancia'] = df_base['total_venta'] - df_base['costo_total']
        
        # 6. Cálculo de Días de Entrega
        df_base['fecha_entrega'] = pd.to_datetime(df_base['fecha_entrega'], errors='coerce')
        df_base['fecha'] = pd.to_datetime(df_base['fecha'])
        
        # Solo calculamos si tenemos fecha de entrega (no es NaT)
        df_base['dias_entrega'] = (df_base['fecha_entrega'] - df_base['fecha']).dt.days
        df_base['dias_entrega'] = df_base['dias_entrega'].fillna(0).astype(int)

        # Si hay conflicto de nombres de estado (pedido vs envio), priorizamos pedido o combinamos
        if 'estado' in df_base.columns:
            df_base.rename(columns={'estado': 'estado_pedido'}, inplace=True)
        
        # Selección final
        rename_map = {
            'cantidad': 'cantidad_vendida',
            'costo_promedio': 'costo_unitario'
        }
        df_base.rename(columns=rename_map, inplace=True)
        
        final_cols = [
            'id_tiempo', 'id_cliente', 'id_producto', 'id_empleado', 'id_ubicacion', 
            'id_pedido', 
            'cantidad_vendida', 'precio_unitario', 'total_venta', 'costo_unitario', 'margen_ganancia',
            'estado_pedido', 'fecha_entrega', 'dias_entrega'
        ]
        
        # Rellenar columnas faltantes por seguridad
        for col in final_cols:
            if col not in df_base.columns:
                df_base[col] = None
                
        return df_base[final_cols]

    def load_data(self, df, table_name):
        if not df.empty:
            print(f"   > Cargando {table_name}: {len(df)} filas...")
            # Usamos 'append' para seguridad o 'replace' si queremos limpiar todo antes
            df.to_sql(table_name, self.target_engine, if_exists='replace', index=False)
        else:
            print(f"⚠ {table_name} vacía.")

    def run_etl(self):
        print("=== INICIO PROCESO ETL ===")
        
        # 1. Extract
        data = self.extract_data()
        
        # 2. Transform
        print("\n--- Transformando ---")
        dim_tiempo = self.transform_dim_tiempo(data['pedidos'], data['envios'])
        dim_ubicacion = self.transform_dim_ubicacion(data['clientes'])
        dim_producto = self.transform_dim_producto(data['productos'], data['categorias'], data['proveedores'])
        
        dim_empleado = data['empleados'].copy()
        dim_cliente = data['clientes'].copy()
        
        hechos_ventas = self.transform_hechos_ventas(data, dim_tiempo, dim_ubicacion)
        
        # 3. Load
        print("\n--- Cargando al DW ---")
        self.load_data(dim_tiempo, 'dim_tiempo')
        self.load_data(dim_ubicacion, 'dim_ubicacion')
        self.load_data(dim_producto, 'dim_producto')
        self.load_data(dim_empleado, 'dim_empleado')
        self.load_data(dim_cliente, 'dim_cliente')
        self.load_data(hechos_ventas, 'hechos_ventas')
        
        print("\n=== FIN CORRECTO ===")

# Ejecución del ETL
if __name__ == "__main__":
    
    # Configurar conexiones a las bases de datos
    # OLTP: Base de datos transaccional origen
    SOURCE = "postgresql://admin:admin123@localhost:5432/techzone_OLTP"
    
    # DW: Data Warehouse destino
    TARGET = "postgresql://admin:admin123@localhost:5434/techzone_DW"
    
    # Crear instancia del ETL y ejecutar
    etl = ETLProcessor(SOURCE, TARGET)
    etl.run_etl()