
import pandas as pd
import glob
import os

#CARGA DE DATOS

#Verificar si los archivos existen en la carpeta DATA
archivos=glob.glob('data/ecommerce_*.csv')

if not archivos:
    print("No se encontraron los archivos.")
else:
    print(f"Archivos encontrados: {len(archivos)}")
    for f in sorted(archivos):
        print(f"-{os.path.basename(f)}")
        

#Cargar CSV principales
df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')

#================================================================

#EXPLORACION DE DATOS

#Exploracion con loop de exploracion
datasets = {
    "orders": df_orders,
    "order_items": df_order_items,
    "customers": df_customers,
    "products": df_products
}

for name, df in datasets.items():
    print(f"Dataset: {name}")
    print(f"Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
    print(df.head())
    print(df.info())

#================================================================

#PERFILADO DE DATOS (nulos y duplicados)
    
#Manejo de nulos para cada dataset
for name, df in datasets.items():
    print(f"Dataset: {name}")
    print("-" * 40)

    #Nulos
    print("Valores nulos:")
    print(df.isnull().sum())

    print("% de nulos:")
    print((df.isnull().sum() / len(df)) * 100)

    #Duplicados
    print("Duplicados:")
    print(df.duplicated().sum())
    
#Validación de nulos (orders)
print(df_orders["promotion_id"].isnull().sum())
print(df_orders["notes"].isnull().sum())

#===============================================================
#TRANSFORMACIÓN

#Creación de variable útil para promotion-id
df_orders["has_promotion"] = df_orders["promotion_id"].notnull()

#Limpiar texto opcional para notes
df_orders["notes"] = df_orders["notes"].fillna("")

#VALIDACIÓN DE CAMBIOS POST TRANSFORMACIÓN
print(df_orders[["promotion_id", "has_promotion"]].head())

print("Nulos en has_promotion:", df_orders["has_promotion"].isnull().sum())

#TRANSFORMACIÓN DE FECHAS A DATETIME
df_orders["order_date"] = pd.to_datetime(df_orders["order_date"])
df_customers["birth_date"] = pd.to_datetime(df_customers["birth_date"])
df_products["created_at"] = pd.to_datetime(df_products["created_at"])
print("Conversión de fechas realizada")

#===============================================================

#MÉTRICAS
df_orders.describe()
df_order_items.describe()

#================================================================

#PREGUNTAS DE NEGOCIO

#1. Top 5 clientes por gasto total- Agrupar por customer_id y sumar total_amount

ventas_cliente = df_orders.groupby('customer_id').agg({
  'total_amount': 'sum',
  'order_id': 'count'
}).rename(columns={'total_amount': 'total_gastado', 'order_id': 'cantidad_ordenes'})
ventas_cliente = ventas_cliente.sort_values('total_gastado', ascending=False)
print("Top 5 clientes:")
print(ventas_cliente.head())

#2.Producto más vendido- Unir orders con orders item para QY y agrupar por product_id y sumar QY
productos_vendidos = df_order_items.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
print(f"Producto más vendido: ID {productos_vendidos.idxmax()} ({productos_vendidos.max()} unidades)")

#3.Evolución mensual de ventas- Agrupar por mes y sumar total_amount
df_orders['mes'] = df_orders['order_date'].dt.to_period('M')
ventas_mes = df_orders.groupby('mes')['total_amount'].sum().reset_index()
ventas_mes.columns = ['mes', 'total_ventas']
print("Ventas por mes:")
print(ventas_mes)


#=======================================================================

#LOAD INVERSO

# Crear carpeta output si no existe
import os
os.makedirs('output', exist_ok=True)

# Guardar métricas en CSV
ventas_cliente.to_csv('output/ventas_por_cliente.csv', index=False)
ventas_mes.to_csv('output/ventas_por_mes.csv', index=False)

# Guardar datos limpios
df_orders.to_csv('output/orders_clean.csv', index=False)

print("Archivos CSV guardados en output/")

# Guardar en Parquet
df_orders.to_parquet('output/orders_clean.parquet', index=False)

# Comparar tamaños
csv_size = os.path.getsize('output/orders_clean.csv') / 1024
parquet_size = os.path.getsize('output/orders_clean.parquet') / 1024

print(f"Tamaño CSV: {csv_size:.1f} KB")
print(f"Tamaño Parquet: {parquet_size:.1f} KB")
print(f"Parquet es {csv_size/parquet_size:.1f}x más chico")