# TechZone ETL Pipeline - Sistema de Inteligencia de Negocios

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791.svg)](https://www.postgresql.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0.44-red.svg)](https://www.sqlalchemy.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.3.3-150458.svg)](https://pandas.pydata.org/)

Un pipeline ETL (Extracción-Transformación-Carga) listo para producción que transforma datos transaccionales de una base de datos OLTP en un Data Warehouse con esquema estrella optimizado para inteligencia de negocios y análisis.

## 📋 Tabla de Contenidos

- [Resumen](#resumen)
- [Contexto de Negocio](#contexto-de-negocio)
- [Arquitectura Técnica](#arquitectura-técnica)
- [Características Principales](#características-principales)
- [Tecnologías](#tecnologías)
- [Modelos de Base de Datos](#modelos-de-base-de-datos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Detalles del Pipeline ETL](#detalles-del-pipeline-etl)
- [Métricas de Negocio](#métricas-de-negocio)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Habilidades Demostradas](#habilidades-demostradas)

## 🎯 Resumen

Este proyecto implementa una solución ETL completa que extrae datos de una base de datos OLTP transaccional (sistema retail TechZone), los transforma mediante lógica de negocio sofisticada, y los carga en un Data Warehouse dimensional usando un diseño de esquema estrella. El Data Warehouse resultante permite consultas avanzadas de inteligencia de negocios y análisis.

## 🏢 Contexto de Negocio

**TechZone** es una tienda minorista de computadoras que gestiona:
- Relaciones con clientes y ventas
- Inventario de productos con categorías
- Gestión de proveedores
- Seguimiento del desempeño de empleados
- Procesamiento y cumplimiento de pedidos
- Operaciones de envío y entrega
- Procesamiento de pagos

El pipeline ETL consolida estos datos operacionales en estructuras analíticas que apoyan la toma de decisiones estratégicas.

## 🏗️ Arquitectura Técnica

### Origen: Base de Datos OLTP (techzone_OLTP)
- **Tipo:** Base de datos relacional normalizada (3FN)
- **Propósito:** Procesamiento de transacciones
- **Tablas:** 11 tablas operacionales
- **Puerto:** 5432

### Destino: Data Warehouse (techzone_DW)
- **Tipo:** Modelo dimensional (Esquema Estrella)
- **Propósito:** Análisis y reportes
- **Estructura:** 1 tabla de hechos + 5 tablas de dimensiones
- **Puerto:** 5434

### Flujo ETL
```
Base de Datos OLTP → Extracción → Transformación → Carga → Data Warehouse
        ↓                            ↓                           ↓
  Datos Operacionales        Lógica de Negocio          Datos Analíticos
```

## ✨ Características Principales

### Extracción de Datos
- Extracción automatizada de 9 tablas de origen
- Extracción tolerante a errores con manejo de excepciones
- Connection pooling para rendimiento óptimo

### Transformación de Datos
1. **Dimensión Tiempo (dim_tiempo)**
   - Genera atributos de fecha completos
   - Soporte de idioma español para meses y días
   - Características de calendario de negocio (trimestres, semestres, fines de semana)

2. **Dimensión Ubicación (dim_ubicacion)**
   - Análisis de datos geográficos desde direcciones
   - Jerarquía de ubicación estandarizada

3. **Dimensión Producto (dim_producto)**
   - Enriquecido con información de categoría y proveedor
   - Manejo elegante de relaciones faltantes

4. **Análisis de Costos**
   - Calcula costos unitarios promedio desde órdenes de compra
   - Estimación inteligente de respaldo (70% del precio de venta)

5. **Tabla de Hechos de Ventas (hechos_ventas)**
   - Registros completos de transacciones de venta
   - Métricas de negocio calculadas:
     - Ingresos totales
     - Costos totales
     - Márgenes de ganancia
     - Análisis de tiempo de entrega
   - Preserva integridad de datos con estrategia LEFT JOIN

### Carga de Datos
- Estrategia de carga con refresco completo
- Registro de progreso y monitoreo
- Integridad transaccional

## 🛠️ Tecnologías

| Categoría | Tecnología | Versión |
|-----------|-----------|---------|
| **Lenguaje** | Python | 3.12 |
| **Base de Datos** | PostgreSQL | 13+ |
| **ORM** | SQLAlchemy | 2.0.44 |
| **Procesamiento de Datos** | Pandas | 2.3.3 |
| **Computación Numérica** | NumPy | 2.3.5 |
| **Adaptador de BD** | psycopg2-binary | 2.9.11 |
| **Utilidades de Fecha** | python-dateutil | 2.9.0 |
| **Soporte de Zona Horaria** | pytz | 2025.2 |

## 📊 Modelos de Base de Datos

### Modelo OLTP
![Diagrama Entidad-Relación OLTP](Model_OLTP.png)

El modelo OLTP representa una base de datos transaccional normalizada con entidades para clientes, productos, pedidos, empleados, proveedores y datos operacionales relacionados.

### Modelo Data Warehouse
![Esquema Estrella del Data Warehouse](Model_DW.png)

El Data Warehouse implementa un esquema estrella con:
- **Tabla de Hechos:** `hechos_ventas` (transacciones de venta)
- **Tablas de Dimensiones:**
  - `dim_tiempo` (tiempo)
  - `dim_cliente` (cliente)
  - `dim_producto` (producto)
  - `dim_empleado` (empleado)
  - `dim_ubicacion` (ubicación)

## 🚀 Instalación

### Prerrequisitos
- Python 3.12+
- PostgreSQL 13+
- pip (gestor de paquetes de Python)

### Configuración

1. **Clonar el repositorio**
```bash
git clone <url-del-repositorio>
cd ETL
```

2. **Crear entorno virtual**
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. **Instalar dependencias**
```bash
pip install pandas==2.3.3 numpy==2.3.5 sqlalchemy==2.0.44 psycopg2-binary==2.9.11 python-dateutil==2.9.0 pytz==2025.2
```

4. **Configurar bases de datos**
```bash
# Crear base de datos OLTP
psql -U postgres -p 5432 -f techzone_OLTP

# Crear Data Warehouse
psql -U postgres -p 5434 -f techzone_DW
```

5. **Configurar conexiones de base de datos**

Editar las cadenas de conexión en `ETL_OLTP_to_DW.py`:
```python
SOURCE = "postgresql://admin:admin123@localhost:5432/techzone_OLTP"
TARGET = "postgresql://admin:admin123@localhost:5434/techzone_DW"
```

## 💻 Uso

Ejecutar el pipeline ETL:

```bash
python ETL_OLTP_to_DW.py
```

### Salida Esperada
```
Iniciando proceso ETL...
Extrayendo datos de la base de datos OLTP...
✓ Extracción completada
Transformando dim_tiempo...
✓ dim_tiempo transformada: 150 registros
Transformando dim_ubicacion...
✓ dim_ubicacion transformada: 45 registros
Transformando dim_producto...
✓ dim_producto transformada: 89 registros
Transformando hechos_ventas...
✓ hechos_ventas transformada: 523 registros
Cargando datos al Data Warehouse...
✓ Carga completada
ETL completado exitosamente
```

## 🔄 Detalles del Pipeline ETL

### 1. Fase de Extracción
- Se conecta a la base de datos OLTP de origen
- Lee 9 tablas en DataFrames de Pandas
- Valida la extracción de datos

### 2. Fase de Transformación

#### Dimensión Tiempo
- Extrae fechas únicas de pedidos y envíos
- Genera atributos: día, mes, año, trimestre, semestre, número de semana
- Agrega etiquetas en idioma español
- Marca fines de semana y días festivos

#### Dimensión Ubicación
- Analiza direcciones de clientes
- Extrae: calle, ciudad, estado, código postal, país
- Estandariza datos geográficos

#### Dimensión Producto
- Enriquece productos con nombres de categoría y proveedores
- Maneja categoría faltante: "Sin Categoría"
- Maneja proveedor faltante: "Sin Proveedor"

#### Análisis de Costos
- Calcula costo unitario promedio desde órdenes de compra
- Para productos sin datos de compra: costo estimado = precio de venta × 0.70

#### Tabla de Hechos de Ventas
- Une pedidos, detalles de pedido, productos y envíos
- Calcula:
  - `monto_total`: ingresos totales por transacción
  - `costo_total`: costo total por transacción
  - `margen_ganancia`: margen de ganancia
  - `tiempo_entrega_dias`: tiempo de entrega en días
- Usa LEFT JOIN para envíos para preservar pedidos recientes

### 3. Fase de Carga
- Carga todas las tablas transformadas al Data Warehouse
- Usa `if_exists='replace'` para refresco completo
- Mantiene integridad referencial

## 📈 Métricas de Negocio

El Data Warehouse permite análisis de:

- **Desempeño de Ventas:** Tendencias de ingresos por período, producto, empleado, cliente
- **Rentabilidad:** Análisis de márgenes, seguimiento de costos, optimización de ganancias
- **Eficiencia Operacional:** Métricas de tiempo de entrega, tasas de cumplimiento de pedidos
- **Análisis de Productos:** Desempeño de categorías, análisis de proveedores
- **Insights de Clientes:** Patrones de compra, análisis de lealtad
- **Desempeño de Empleados:** Ventas por empleado, métricas de productividad
- **Tendencias Estacionales:** Patrones trimestrales/mensuales/semanales

### Consultas Analíticas de Ejemplo

**Top 5 productos por margen de ganancia:**
```sql
SELECT p.nombre_producto, AVG(h.margen_ganancia) as margen_promedio
FROM hechos_ventas h
JOIN dim_producto p ON h.id_producto = p.id_producto
GROUP BY p.nombre_producto
ORDER BY margen_promedio DESC
LIMIT 5;
```

**Tendencia de ventas mensual:**
```sql
SELECT t.mes, t.año, SUM(h.monto_total) as ventas_totales
FROM hechos_ventas h
JOIN dim_tiempo t ON h.id_tiempo = t.id_tiempo
GROUP BY t.año, t.mes, t.num_mes
ORDER BY t.año, t.num_mes;
```

## 📁 Estructura del Proyecto

```
ETL/
├── ETL_OLTP_to_DW.py          # Script principal del pipeline ETL (237 líneas)
├── techzone_OLTP              # Esquema SQL de base de datos origen y datos de ejemplo
├── techzone_DW                # Esquema SQL del data warehouse destino
├── Model_OLTP.png             # Diagrama ER de base de datos OLTP
├── Model_DW.png               # Diagrama de esquema estrella del Data Warehouse
├── Enunciado                  # Documento de requerimientos del proyecto
├── venv/                      # Entorno virtual de Python
└── README.md                  # Este archivo
```

## 🎓 Habilidades Demostradas

Este proyecto demuestra experiencia en:

### Ingeniería de Datos
- Desarrollo y orquestación de pipelines ETL
- Diseño e implementación de data warehouse
- Modelado dimensional con esquema estrella
- Gestión y validación de calidad de datos

### Tecnologías de Bases de Datos
- Administración de bases de datos PostgreSQL
- Optimización de consultas SQL
- Estrategias de indexación de bases de datos
- Gestión de conexiones multi-base de datos

### Desarrollo en Python
- Programación orientada a objetos
- ORM SQLAlchemy
- Manipulación de datos con Pandas
- Manejo de errores y logging
- Principios de código limpio

### Inteligencia de Negocios
- Modelado dimensional
- Cálculo de métricas de negocio
- Estructuras de datos listas para análisis
- Fundamentos de reportería BI

### Ingeniería de Software
- Organización y modularidad de código
- Documentación y comentarios
- Listo para control de versiones
- Calidad de código lista para producción

---

## 📧 Contacto

**Santino Giampietro**

¡Para preguntas u oportunidades de colaboración, no dudes en contactarme!

---

## 📄 Licencia

Este proyecto está disponible para fines de portafolio y educacionales.

---

**Nota:** Este proyecto demuestra habilidades de ingeniería de datos del mundo real aplicables a roles de inteligencia de negocios, análisis de datos y data warehousing.