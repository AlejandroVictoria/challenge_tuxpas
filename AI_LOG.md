# Bitácora de Uso de IA (AI_LOG.md)

## 1. Herramientas Utilizadas

- **Editor/Entorno:** Visual Studio Code y Gemini Web App
- **Modelos de Lenguaje:** Gemini 3.8 Flash (Asistencia y Consultas).
- **Herramientas Auxiliares:** GitHub Copilot (Autocompletado de líneas de código).

---

## 2. Flujo de Trabajo y Orquestación

La forma en la que trabajo con un LLM la puedo describir en los siguientes pasos:

- **Sesión de planeación y definición de objetivos:** Defino el objetivo principal y los entregables esperados, de manera general. En este caso, se definió _la creación de un Data Lake usando servicios de AWS que procese la información de ventas del problema planteado_. Dando un panorama general de los pasos a considerar, los componentes que pueden intervenir y se consideran las entradas y salidas de cada componente. Esta sesión a su vez es una lluvia de ideas, donde el LLM cumple el rol de contraparte, validamos que las ideas propuestas tengan lógica y dirección hacía la realización del objetivo principal.
- **Definición de entregables:** Luego de la planeación y definición de objetivos, así como la definición de componentes clave para el desarrollo, solicito la creación de un cronograma donde las actividades a desarrollar sigan la dirección y lógica establecida en el paso anterior. Con esta información, solicito al LLM proponer una duración para cada actividad.
- **Desarrollo y Pruebas:** Teniendo el proyecto dividido por componentes, junto con sus variables de entrada y salida, así como una descripción del objetivo de cada componente, procedo a la construcción módulo a módulo del proyecto. Busco tener el contexto suficiente para indicarle al LLM las herramientas base para el desarrollo: funciones auxiliares, librerias, servicios de aws, o APIs que puedan ayudarnos a cumplir el objetivo y, claro, sean de mi conocimiento. Esta etapa suele tener más fricciones y, en ocasiones, ser de avance lento pues mi rol es validar que el código entregado cumpla con el flujo lógico propuesto en el planteamiento.
- **Documentación de errores:** A lo largo del desarrollo es muy común encontrarse con elementos que no se consideraron en la planeación. En esta etapa, que suele ser paralela al desarrollo, decido si los elementos tienen un rol fundamental en el flujo y que, sin ellos, se puede romper dicho flujo. De ser cierta la premisa anterior, se considera agregar a los nuevos elementos, de lo contrario, los registro como componentes a considerar para una posible segunda versión.
- **Documentación final:** Luego de haber concluido la etapa de desarrollo y documentación de errores, cotejo los componentes y el alcance de la planeación contra el resultado. Evalúo los elementos que hayan sido agregados durante esta etapa y los documento.

---

## 3. Prompts Clave e Interacciones

### Interacción 1: Definición de servicio a usar

- **Prompt enviado:**

  > "Vamos a lo interesante. En la pequeña investigación que hice ayer sobre la arquitectura que planteamos, encontré el servicio de CloudFormation para centralizar la construcción del ambiente. Como ya tenemos experiencia con este tipo de configuraciones y servicios, vamos a continuar con la dinámica que construimos para el lakehouse, centralizar la arquitectura en archivos de configuración. Así nos evitamos estar dando clicks en la consola y nos dedicamos a depurar los servicios. Tenemos dos caminos, pero aún no sé cuál es mejor: usar el archivo .yaml o usar el CDK."

- **Resultado de la IA (Resumido):**
  El modelo analizó ambas opciones mediante una matriz comparativa (CloudFormation YAML vs. AWS CDK), evaluando criterios como tiempo de configuración, complejidad del repositorio, legibilidad para el evaluador y control de costos. Recomendó utilizar **CloudFormation nativo en YAML**, argumentando que disminuye la sobrecarga de dependencias (evitando fases de _bootstrapping_), mantiene el repositorio limpio y facilita una auditoría inmediata por parte del equipo evaluador.
- **Acción y Justificación:** **Aceptada tal cual la propuesta**. Tomé la recomendación de utilizar un único archivo `template.yaml`. Mi justificación coincide con el argumento del modelo: _en un reto técnico con tiempos acotados y restricciones presupuestarias, la prioridad debe ser el core del pipeline de datos (procesamiento de datos y modelado en Glue/Athena) en lugar de la gestión de dependencias de un entorno CDK_. Esto permitió entregar una arquitectura 100% explícita, auditable de un vistazo y de fácil mantenimiento en un solo punto del repositorio.

---

### Interacción 2: Definición de estrategia de ingesta (Manual/Automática)

- **Prompt enviado:**

  > "Ya estamos acá de nuevo para seguir trabajando. Estuve pensando en algo en la noche. Tuve una idea de automatizar toda la ingesta de datos tomándolos desde su tienda en línea, pero en el planteamiento ya nos indican que se "consolida", de cierto modo, la información de las más de 40 tiendas en 3 puntos específicos. No haría falta crear un pipeline que comience el ciclo de vida de los datos desde las tiendas, más bien, sería desde esos tres puntos. En todo caso, me surgen dos dudas, ¿cada cuándo se actualizan esos datos en los 3 puntos? y ¿tendríamos que diseñar dos modos de ingesta? una "manual" que permita mover los datos como los archivos que nos proporcionaron y luego ya la ingesta automatizada. Al final, creo que si hay que buscar la manera de implementar los dos modos de ingesta, manual y automática. Pero lo automático choca con mi primera duda. Hará falta preguntar al "cliente" para aclarar al menos la primer duda."

- **Resultado de la IA (Resumido):**
  El asistente validó delimitar el pipeline a partir de los 3 puntos consolidados para evitar sobreingeniería hacia las tiendas individuales. Recomendó resolver la ambigüedad de la cadencia asumiendo una ventana de consolidación diaria (T+1 / batch nocturno) respaldada en la naturaleza de los orígenes (cierre POS, snapshot de inventario y export de Shopify). Propuso unificar la ingesta manual y automática bajo un mismo motor desacoplado del disparador (CLI/Backfill vs. S3 Event/Cron EventBridge) garantizando idempotencia por partición de fecha.
- **Acción y Justificación:** **Modificado**. Acepté el supuesto de la consolidación diaria al cierre del día y el diseño de ingesta híbrida idempotente propuesto por el LLM. Sin embargo, acoté y modifiqué la implementación práctica:

1. Descarté preguntar al cliente y establecí documentar los supuestos de negocio y umbrales en un archivo de configuración centralizado (`config.yaml`/`settings.py`) para desacoplarlos del código.
2. En lugar de procesar archivos locales dispersos, decidí modelar la arquitectura simulando los 3 puntos externos mediante interfaces/buckets dedicados (`pos/`, `inventory/`, `shopify/`), dejando que un worker nocturno consolide la información hacia el Lakehouse, cubriendo así el alcance de negocio sin sobrediseñar conectores individuales para cada tienda.

---

### Interacción 3: Automatización de dependencias S3/Glue

- **Prompt enviado:**

  > "Por qué no agregamos la configuración del recurso en el template? podemos establecer relaciones de dependencia dentro del template, vamos a agregar la configuración para que se cree un archivo dumb vacío pero con el nombre necesario"

- **Resultado de la IA (Resumido):**
  El asistente explicó que CloudFormation carece de un recurso declarativo nativo para escribir objetos en S3 (`AWS::S3::Object`), por lo que propuso el patrón estándar empresarial: implementar un _Custom Resource_ (`Custom::S3InitScripts`) respaldado por una función AWS Lambda efímera en Python inline. Este componente genera los archivos placeholder (`raw_to_silver.py`, `silver_to_gold.py`) en S3 y desbloquea la creación de los Glue Jobs mediante directivas explícitas `DependsOn`.
- **Acción y Justificación:** **Aceptado tal cual**. Incorporé la solución de la Lambda auxiliar efímera y el recurso personalizado en el `template.yaml`. Mi criterio fue priorizar la reproducibilidad y el principio de _Zero-Touch Deployment_: eliminar por completo la intervención manual en la terminal (subir archivos por CLI entre fases) y evitar fallos por dependencias circulares durante el aprovisionamiento inicial del stack.

---

## 4. Gestión de Errores y Alucinaciones

- **Contexto del problema:** Durante las validaciones de los ETL entre capas del Data Lake, me percaté que las particiones no se estaban guardando correctamente. Habíamos definido un esquema de guardado del tipo "<carpeta>/dt=<yyyy-mm-dd/<archivos>". Sin embargo los scripts definidos no generaban las particiones colapsándolas en una única carpeta "<carpeta>/dt=...". Causando conflicto pues no se creaban correctamente las tablas, cortando el flujo del pipeline.
- **El error de la IA:** El LLM no identificó correctamente la solución y siempre apelaba a reforzar la variable que contiene la información de las particiones. Validando que se generara correctamente y proponiendo diferentes formas de escribir el método pyarrow.dataset.write_dataset():

#### CÓDIGO CAUSANTE DE LA FALLA

```python
import pyarrow.dataset as ds
import pyarrow.fs as pafs

s3_fs = pafs.S3FileSystem()
bucket_and_key = output_path.replace("s3://", "")  # Ej: "cafenorte-lakehouse-dev/silver/fct_pos_sales"

ds.write_dataset(
    data=arrow_table,
    base_dir=bucket_and_key,
    filesystem=s3_fs,
    format="parquet",
    partitioning=partition_cols,       # ["dt"] o ["year_month"]
    partitioning_flavor="hive",
    file_options=ds.ParquetFileFormat().make_write_options(compression="snappy"),
    existing_data_behavior="overwrite_or_ignore",
)
```

- **Detección:** Los logs de AWS Glue fallaban al querer implementar un join en las tablas de la capa Silver. Validando las tablas que alimentaban a la instrucción, se encontró que en S3 sólo existía un registro "<carpeta>/dt=..." que no se detectó antes porque en local si se generaban todos los registros. Se rastreó la generación de las particiones hasta dar con el comando write_dataset().
- **Corrección:** Dado que el LLM no ofrecía otras soluciones más que reforzar la variable donde se guardaban los parámetros de las particiones, o cambiar el método de escribir los datos, me dediqué a investigar problemas relacionados con el método en cuestión en el contexto de AWS Glue. Encontré diversas fuentes que indicaban que dicha función con dicho parámetro tienen problemas para ejecutarse en el entorno de AWS (https://arrow.apache.org/cookbook/py/io.html#writing-partitioned-datasets, https://github.com/apache/arrow/issues/38794, https://github.com/apache/arrow/issues/20090, https://docs.aws.amazon.com/glue/latest/dg/glue-troubleshooting-errors.html#error-update-from-job-partitions). Al final, le propuse al LLM gestionar manualmente las particiones para resolver el problema.

---

## 5. Autocrítica Final

Considero que el uso de LLMs en el desarrollo de soluciones de software es una herramienta que potencia inmensamente la capacidad del que la usa, siempre y cuando la persona tenga las herramientas y contexto necesario para el objetivo que busca alcanzar. Si analizo desde una perspectiva donde el 100% del trabajo se refleja en la escritura de código, considero que el proyecto se divide en 80%, incluso más, de trabajo hecho por el LLM.
Pensando en retrospectiva, y tomando en cuenta otros factores como decisiones clave, flujo propuesto, depuración de código, elección de servicios, herramientas, librerías, etc. adopté una postura pragmática al delegar 60% de la escritura de código e implementación al LLM. Por mi parte, mis esfuerzos estuvieron centrados en el diseño de la arquitectura, dirección y apego a los objetivos, y corrección de fallas en momentos clave.
