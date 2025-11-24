Ejercicio Práctico: Diseño de Data Warehouse para "TechZone"

Contexto del Negocio: La empresa de venta de hardware TechZone ha operado exitosamente con su sistema transaccional actual (OLTP) durante varios años. Sin embargo, el Gerente Comercial reporta que las consultas de reportes históricos son cada vez más lentas y complejas. Actualmente, para saber cuánto se vendió de una categoría específica, el equipo de IT debe cruzar 4 o 5 tablas, lo que colapsa el servidor en horarios pico.

Objetivo: Se le solicita diseñar y poblar un Data Warehouse (Modelo Estrella) centrado en el análisis de Ventas, partiendo del modelo relacional actual.

Requerimientos Analíticos (User Stories):

    Análisis de Producto Simplificado (Desnormalización): La gerencia necesita analizar el rendimiento de los productos, pero quiere evitar la complejidad de las jerarquías actuales.

        Requerimiento: En el modelo dimensional, la información del Producto, su Categoría y su Proveedor debe estar consolidada en una única dimensión para facilitar el filtrado rápido.

    Inteligencia Temporal (Time Intelligence): No basta con saber la fecha de la venta. Marketing necesita evaluar patrones de compra estacionales.

        Requerimiento: Se requiere una dimensión de tiempo robusta que permita agrupar ventas no solo por fecha, sino por Trimestre, Semestre, Día de la semana y una bandera que indique si la venta ocurrió en Fin de Semana.

    Análisis Geográfico: Se quiere analizar la penetración del mercado por regiones geográficas, independientemente de quién sea el cliente específico.

        Requerimiento: Extraer la información de ubicación (Ciudad, Región, País) y separarla en su propia dimensión geográfica (dim_ubicacion), normalizando los datos que actualmente residen como texto libre en la tabla de clientes.

    Métricas de Rentabilidad y Eficiencia (KPIs): El reporte final no debe requerir cálculos en tiempo de ejecución.

        Requerimiento: La tabla de hechos debe contener pre-calculados:

            El Total de la Venta (Cantidad × Precio).

            El Margen de Ganancia (Venta - Costo).

            Tiempos de entrega (diferencia entre fecha de pedido y entrega).

    Granularidad: El análisis debe permitir bajar hasta el máximo nivel de detalle posible de cada transacción.

        Requerimiento: El grano de la tabla de hechos debe ser por línea de producto dentro de cada pedido (basado en detalle_pedido).

Entregables:

    Diagrama del Modelo Dimensional (Estrella) resultante.

    Script ETL sugerido para poblar las dimensiones y la tabla de hechos.