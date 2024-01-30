--------------------------------------------------
| Historial de eventos en eCommerce de cosmética |
--------------------------------------------------

Este proyecto de PySpark está diseñado para realizar análisis de datos sobre los eventos de una tienda 
eCommerce de comsética. La estructura del proyecto consta de las siguientes carpetas:

## Estructura de Carpetas

- **commons:** Contiene archivos comunes utilizados en el proyecto.
  - 'utils.py': Archivo que proporciona utilidades comunes, como expresiones regulares, configuración de carpetas y clases para manipulación de datos.
  - '__init__.py': Archivo de inicialización de Python.

- **data:** Contiene conjuntos de datos utilizados en el análisis.
  - '2019-Oct.csv': Datos correspondientes a octubre de 2019.
  - '2020-Jan.csv': Datos correspondientes a enero de 2020.

- **src:** Contiene archivos Python que implementan el código de análisis utilizando PySpark.
  - Archivos '.py': Implementan operaciones de extracción, transformación y carga utilizando Spark.

        - 'Fracaso.py' analiza los productos que son introducidos en el producto pero, al final, no se acaban comprando
           y se eliminan del carrito.
        - 'Marcas_populares.py': Extrae las marcas con mayores ventas e ingresos.
        - 'Picos_actividad.py': Consigue las horas y días de la semana con mayor actividad y ventas.
        - 'Ventas.py':

- **outputs:** Directorio donde se almacenan los resultados de las operaciones y análisis. En este caso, el único archivo
    con outputs es 'Ventas.py'.


## Uso del Proyecto

El proyecto utiliza PySpark para realizar análisis de los datos.
Cada archivo en la carpeta 'src' implementa un paso particular proporcionando información diferente sobre el eCommerce
como las ventas, la tasa de fracaso de productos, los picos de actividad y las marcas con mayores ventas.
Para ejecutar el proyecto, se recomienda seguir las siguientes instrucciones.

## Instrucciones de Ejecución

1. Asegúrese de tener PySpark instalado en su entorno.
2. Ejecute los archivos Python en la carpeta 'src' en el orden deseado.
3. Los arhivos 'Marcas_populares.py','Picos_actividad.py' y 'Ventas.py' se ejecutan introduciendo en la
    terminal 'spark-submit <src/05/archivo.py>'. El archivo 'Fracaso.py' se ejecuta introduciendo en la
    terminal 'spark-submit 'spark-submit src/Fracaso.py data/2019-Oct.csv data/2020-Jan.csv'
    El motivo de esta diferencia es para enseñar que existen diferentes maneras que introducir los datos en
    un archivo .py dando la posibilidad de crear un archivo .py genérico que admita diferentes conjuntos de datos.


## Contribuciones

Las contribuciones al proyecto son bienvenidas. Si desea contribuir, haga un fork del proyecto, realice sus cambios y envíe un pull request.


