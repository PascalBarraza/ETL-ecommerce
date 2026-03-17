## 🚀 Proyecto de Ingeniería de Datos

#Pipeline de ETL con python y pandas para procesamiento de datos de e-commerce

##Descripción

Este proyecto consiste en la construcción de un pipeline ETL utilizando Python y pandas para procesar datos de un e-commerce.

El objetivo es cargar, explorar, limpiar y analizar los datos para generar métricas de negocio relevantes.

---

##Tecnologías utilizadas

* Python
* pandas
* Spyder

---

##Estructura del proyecto

* `etl.py`: Script principal del proceso ETL
* `data/`: Carpeta con archivos CSV de entrada (Por motivos de seguridad no se ha cargado la carpeta)

---

##Proceso ETL

### 1. Extracción (Extract)

Se cargan múltiples archivos CSV relacionados a órdenes, clientes, productos y detalle de órdenes.

### 2. Exploración (Explore)

Se realiza un análisis inicial de los datos:

* Cantidad de filas y columnas
* Tipos de datos
* Visualización de primeras filas

### 3. Perfilado de datos

Se analizan:

* Valores nulos
* Porcentaje de nulos
* Registros duplicados

Principales hallazgos:

* `promotion_id` presenta un 75% de valores nulos
* `notes` presenta un 79% de valores nulos

### 4. Transformación (Transform)

Se aplicaron las siguientes transformaciones:

* Creación de la columna `has_promotion`:

  * Indica si una orden tiene promoción asociada
  * Basado en la existencia de `promotion_id`

* Limpieza de la columna `notes`:

  * Reemplazo de valores nulos por string vacío

* Conversión de tipos de datos:

  * `order_date`, `birth_date` y `created_at` convertidos a formato datetime

---

##Análisis realizados (resultado)

Se desarrollaron métricas básicas de negocio:

* Top clientes por gasto total (identificación de clientes con mayor gasto)
* Producto más vendido (detección del producto más vendido)
* Evolución mensual de ventas (Análisis de evolución mensual de ventas)

---

##Decisiones de datos

* No se eliminaron registros con valores nulos, ya que no afectan directamente el análisis
* Se priorizó mantener la integridad del dataset original
* Se generaron variables derivadas para facilitar el análisis

---

##Cómo ejecutar

1. Instalar dependencias:

```bash
pip install pandas
```

2. Ejecutar el script:

```bash
python etl.py
```

---

##Autor

Pascal Barraza Escudero - 2026
