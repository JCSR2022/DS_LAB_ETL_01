# Proyecto Individual 1- Data 04- Soy Henry   
## Data Engineering
![image](https://user-images.githubusercontent.com/108296379/182138583-9011699a-f009-4454-885e-80dca182b6c8.png)


## ¡ Bienvenidos a LABS !
En este primer proyecto proponemos realizar un proceso de ETL (extract, transform and load) a partir de un conjunto de datos que se enfocarán en una misma perspectiva de negocio. Los datos vienen de diversas fuentes de relevamiento de precios en distintos mercados. Deberán trabajar los diferentes tipos de archivos para llevarlos a una misma extensión y, una vez finalizada esta etapa, deberán crear los joins necesarios con el objetivo de crear un DER y dejarlos almacenados en un archivo con extensión .db. Por último, todo su trabajo deberá contemplar la carga incremental del archivo "precios_semana_20200518.txt".

En esta etapa de la academia, verán siempre la palabra "Plus", que hará referencia a los puntos extra disponibles para cada trabajo. Es muy importante que estos sean obviados hasta completar los requerimientos mínimos (deben entregar siempre el producto mínimo viable, MVP, que hace lo mínimo que pedimos), pero siempre recomendamos tratar de completar los plus/adicionales aún después de que el trabajo haya sido entregado, ya que generarán valor agregado a su portfolio.  
    
## Entrega
La entrega se hará mediante en dos canales:
  - Entrega del archivo final, la cuál se hará a partir de un form.
  - Video demostrativo y explicativo de <strong> no más de 5 minutos </strong>. 

## Pasos a seguir (requerimientos mínimos)
- Procesar los diferentes datasets. 
- Crear un archivo DB con el motor de SQL que quieran. Pueden usar SQLAlchemy por ejemplo.
- Realizar en draw.io un diagrama de flujo de trabajo del ETL y explicarlo en vivo.
- Realizar una carga incremental de los archivos que se tienen durante el video.
- Realizar una query en el video, para comprobar el funcionamiento de todo su trabajo. La query a armar es la siguiente: Precio promedio de la sucursal 9-1-688.

## Plus  
En este proyecto, será un plus trabajar con herramientas como Docker, Airflow, NiFi u otras herramientas que optimicen y automaticen el proceso de la consigna, programando las cargas incrementales para realizarse con cierta frecuencia. Recuerden que los plus siempre son totalmente opcionales, va a depender de ustedes si lo quieren realizar o no ¡Desafíense a lograrlo!

## Glosario del trabajo:

### Diccionario de datos

<sub>Spoiler: hay varias inconsistencias (intencionales) en estos archivos.</sub>

Archivos con los relevamientos semanales (ej: precio_semana_20200518.txt)
~~~
Precio: Precio del producto en pesos argentinos.
Vpn_keyproducto_id: Código EAN del producto.
Text_formatsucursal_id: ID de la sucursal. Los dos primeros números determinan la cadena.
~~~

~~~
Productos.parquet  

id: Código EAN del producto.
marca: Marca del producto.
nombre: Detalle del producto.
Presentación: Unidad de presentación del producto.
categoria1: categoria a la que pertenece el producto.
categoria2: categoria a la que pertenece el producto.
categoria3: categoria a la que pertenece el producto.
~~~

~~~
Sucursales.csv  

Id: Es el ID principal, compuesto por la concatenación de banderaId-comercioId-sucursalId.
comercioId: es el identificador de la empresa controlante. Por ejemplo Hipermecado Carrefour y Express tienen mismo comercioId pero distinta banderaId
banderaId: Diferencia las distintas "cadenas" de la misma empresa. Vea, Disco y jumbo pertenecen al mismo comercio pero tienen distinta bandera.
banderaDescripcion: Nombre de fantasía de la Cadena
comercioRazonSocial: Nombre con la que está registrada legalmente la empresa.
provincia: Provincia donde está ubicado el comercio.
localidadl: Localidad donde está ubicado el comercio.
direccion: Dirección del comercio,
lat: Coordenadas de latitud de la ubicación del comercio
lng: Coordenadas de longitud de la ubicación del comercio
~~~

## Tips:  
- Analizar en que instancia de su ETL van a realizar la limpieza y joins de los datasets.  
- Comentar el código en todo momento.  
- Clonar el repositorio, no forkearlo.  
- Denle importancia al README de su trabajo, va a ser su carta de presentación, tanto para con nosotros, como para las empresas que se fijen en su perfil.  
- SUPERTIP: ¡Es el mejor momento para que comiencen a trabajar/familiarizarse con el uso de Github en sus proyectos!  
Expliquen su proyecto, agreguen diagramas de flujo del pipeline y lo que consideren necesario ¡Será parte de su portafolio de proyectos! 

![image](https://www.incworx.com/wp-content/uploads/2021/12/what-is-etl-used-for-1080x675.jpg)

# Muy Importante -> VIDEO DEMO
El video no deberá durar más de 5 minutos, en el mismo, deberán mostrar y explicar el proceso de extracción, transformación y carga de los datos, fundamentación de las herramientas elegidas en conjunto a la demora del trabajo. Por otra parte, deberán exponer la carga incremental asignada.  
En caso de que algún proceso genere tiempo muerto dentro de la grabación, el mismo podrá ser recortado o acelerado para agilizar la exposición.  
  
La grabación deberá ser subida a YouTube en una calidad igual o superior a la correcta legibilidad del código, recomendamos utilizar capturadoras como Loom, Zoom u otras que poseen entradas de audio. No se permite utilizar grabadoras como el telefono, cámaras, etc. Otro paso a seguir para evitar inconvenientes en la corrección es visitar el link donde se produjo la carga del archivo en modo incógnito para asegurar su correcta publicación. 

La descripción del video cargado a YouTube y el nombre del archivo cargado al form deberá ser: NOMBRE Y APELLIDO ESTUDIANTE - DSH - PI_DE  
Ejemplo -> Fulanitode Tal - DSH - PI_DE

### ¡Mucha Suerte!  

<img src = "https://user-images.githubusercontent.com/96025598/188937586-28575753-fbd6-42de-beca-81ae35b659e0.gif" height = 300>


Resumen de las consultas de hoy y panorama general:
Mañana abre el espacio para hacer las entregas. De 12:00 gmt-3 Buenos Aires, a 18:00 gmt-3 Buenos Aires.
No es la finalidad del trabajo la limpieza exhaustiva de los datos.
Nos interesa que puedan tomar los archivos que tienen, y transformarlos para luego poder armar una query y comprobar que la tabla y/o base de datos funciona
También que nos muestren una carga incremental, para también asegurarnos que efectivamente funciona el Pipeline
El miércoles les pasamos los resultados de sus trabajos. Desde el lunes los queremos full enfocados en modelos de ML.
Acuérdense que los videos no pueden superar los 5 minutos.
Si tienen alguna duda, pregunta o consulta, puede responder en este hilo (Aclaración, lo técnico al Slido) :ojos:


Hola Fede!
El video debe durar COMO MÁXIMO 5 minutos. Pueden subirlo a Youtube, Vimeo, Dailymotion, Drive, al servicio que quieran, pero debe tener acceso público (pueden probar abrir el link en una pestaña incógnito y deben poder verlo sin ingresar ningún usuario).
En el formulario de entrega, habrá espacios para colocar el link a su repo de github y el link al video. Ambos deben estar configurados para acceso público.
Si, el readme debe formar parte del repo.
El archivo .db generado (o cualquier otro archivo de backup de la base de datos que generen) debe quedar en el repo de github.
Todo código que hayan utilizado (.ipynb, .py, .sql, etc) debe estar en el repo de github, comentado.
El diagrama realizado en draw.io, LucidChart, Miro, Visio, Canva o cualquier otro pueden exportarlo a una imagen (o sacar una captura de pantalla) y esa imagen la pegan en el readme de su repo de github, o bien la suben aparte pero debe estar en el repo.
Recuerden por favor que tanto el video como su repo de github debe estar configurado para tener acceso publico! Pruebenlo en una pestaña incognito antes de llenar el form por favor.
Muchos éxitos y saludos!