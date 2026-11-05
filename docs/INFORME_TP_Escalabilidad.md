# TP Escalabilidad - Diagramas Actualizados

## DAG
<img width="1207" height="1239" alt="DAG - TP Escalabilidad" src="https://github.com/user-attachments/assets/0341bd01-95d0-404e-830e-64cd4a12730d" />

#### Modificaciones / Nuevas decisiones de diseño
* Se agregaron nodos `ResultAggregator` para cada query. Estos nodos se encargan de bufferear fragmentos de las respuestas a las distintas querys, hasta que éstas estén completas, y finalmente se las puedan devolver al cliente (se envía una respuesta en cuanto esté completa).
* Previamente se tenía un nodo que extraía el vocabulario “raw” desde el abstract de un artículo, y otro nodo que filtraba las stop words. Como decisión de diseño, se juntaron ambos nodos en uno sólo: `VocabularyExtractor`, ya que consideramos que, como de todos modos se deben recorrer todas las palabras del texto, es más rentable ir filtrando las palabras a medida que se recorre el mismo en memoria, que agregar otra fase de mensajería por red.

## Diagrama de Robustez
<img width="1964" height="1115" alt="Diagrama de Robustez - TP Escalabilidad" src="https://github.com/user-attachments/assets/efcce454-c3e8-4be6-b86f-2ff9b04ba11e" />

#### Modificaciones / Nuevas decisiones de diseño
* **Protocolo Cliente-Servidor**: Se emplea un protocolo binario propio. El cliente lee las filas del parquet, las serializa a mapas _[clave,valor]_ que va guardando en batches (tamaño configurable), para enviarlas al gateway sin procesamiento adicional. Para devolver los resultados de las querys, todos los nodos aggregators de resultados publican a la misma queue de la cual lee el gateway, y éste sólo se encarga de forwardear las respuestas completas al cliente.
* **Query 5**: Previamente, para resolver la query 5, la lógica de calcular el promedio y guardar las cantidades de palabras de los demás artículos estaba en un sólo nodo. Tras considerar que esto posiblemente se convierta en un cuello de botella, con mucho consumo de memoria, se separaron las responsabilidades:
   * Un nodo `YearSplitter` es un nodo stateless que se fija si el año es <= ó > a 2020, y en base a ésto envía la data al nodo `OldArticlesAverager` ó `AboveAverageFilter` respectivamente. El envío se realiza a través de un exchange, haciendo uso de hashing por client ID.
   * `OldArticlesAverager` recibe la cantidad de palabras de los artículos <= 2020 para un mismo cliente. Cuando recibe el EOF, le envía el promedio al nodo `AboveAverageFilter` correspondiente al cliente.
   * `AboveAverageFilter` recibe la cantidad de palabras de los artículos >2020 para un mismo cliente. Para avanzar a la siguiente etapa, es necesario que haya recibido tanto el mensaje EOF como el promedio. Finalmente, el envío de los artículos resultados al correspondiente nodo aggregator se hace con streaming de a batches.
 

## Diagrama de Despliegue
<img width="1632" height="552" alt="Diagrama de Despliegue - TP Escalabilidad" src="https://github.com/user-attachments/assets/cfbb5fd0-4804-4c48-bd3b-de81a16b3102" />

* Se agregaron los nodos: `QXResultAggregator`, `VocabularyExtractor`, `YearSplitter`, `OldArticlesAverager`, `AboveAverageFilter`.
* Se quitaron los nodos: `WordCounter`, `StopwordsFilter`.
