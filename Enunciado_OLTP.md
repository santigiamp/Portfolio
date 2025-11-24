“TechZone – Sistema de gestión comercial”

    TechZone es una tienda de insumos informáticos que gestiona clientes, empleados, productos, proveedores, pedidos, facturas, envíos y pagos.

    Cada producto pertenece a una categoría y tiene un proveedor principal.
    Los clientes realizan pedidos que son gestionados por empleados de ventas.
    Cada pedido puede incluir varios productos (detalle del pedido).
    Cuando un pedido se confirma, se genera una factura y un envío, que pueden tener estados distintos (“pendiente”, “en tránsito”, “entregado”).
    Los clientes pueden realizar pagos parciales o totales de sus facturas.
    Los proveedores también emiten órdenes de compra, que sirven para reponer el stock.

    El objetivo de esta base es registrar todas las operaciones cotidianas del negocio, no para análisis directo, sino como base transaccional de un sistema ERP pequeño.


    Entidades y relaciones principales

        Cliente (1) – (N) Pedido

        Empleado (1) – (N) Pedido

        Pedido (1) – (N) DetallePedido – (N) Producto

        Producto (N) – (1) Categoría

        Producto (N) – (1) Proveedor

        Pedido (1) – (1) Factura

        Factura (1) – (N) Pago

        Pedido (1) – (1) Envío

        Proveedor (1) – (N) OrdenCompra

        OrdenCompra (1) – (N) DetalleOrdenCompra – (N) Producto