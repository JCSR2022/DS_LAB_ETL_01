# DS_LAB_ETL_01
Proyecto Individual 1- Data 04- Soy Henry   

El presente proyecto consiste en aplicar el proceso de ETL de las tablas de datos presentadas, en las cuales se tiene información con formatos varidos y se debe asimilar y procesar la misma, se tienen 7 tablas las cuales se pueden descargar en el link https://drive.google.com/drive/folders/1Rsq-HHomPtQwy7RIWQ574wKcf56LiGq1?usp=sharing.

*precios_semanas_20200419_20200426.xlsx
*precios_semana_20200413.csv
*precios_semana_20200503.json
*precios_semana_20200518.csv
*precios_semana_20200518.txt
*producto.parquet
*sucursal.csv

El objetivo al trabajar con las tablas es:
Crear un archivo DB con el motor de MySQL.
Presentar en video el proceso de ETL 
Realizar una carga incremental de los archivos que se tienen durante el video
Realizar una query en el video, para comprobar el funcionamiento de todo su trabajo. La query a armar es la siguiente: Precio promedio de la sucursal 9-1-688.

EL proceso de ETL para las tablas a procesar consiste principalemente en:

1. Carga primiaria en Python par su revison
2. Se borran los datos repetidos si existen y los valores null
3. Se modifica el id de la tabla para lograr que sea un int
4. Se verifica incosistencias en los datos, valores incorrectos o outliers
4. Se exporta la tabla a una base de datos en MySQL
5. Se genera en MySQL las llaves primarias y se vinculan las tablas creando el modelo relacional


Video : https://www.youtube.com/watch?v=EfpD-Le__2Y

