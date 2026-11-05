# TP Escalabilidad - Diagramas Actualizados

## DAG
<img width="1207" height="1239" alt="DAG - TP Escalabilidad" src="https://github.com/user-attachments/assets/0341bd01-95d0-404e-830e-64cd4a12730d" />

#### Modificaciones / Nuevas decisiones de diseño
* Se agregaron nodos `ResultAggregator` para cada query. Estos nodos se encargan de bufferear fragmentos de las respuestas a las distintas querys, hasta que éstas estén completas, y finalmente se las puedan devolver al cliente (se envía una respuesta en cuanto esté completa).
* Previamente se tenía un nodo que extraía el vocabulario “raw” desde el abstract de un artículo, y otro nodo que filtraba las stop words. Como decisión de diseño, se juntaron ambos nodos en uno sólo: `VocabularyExtractor`, ya que consideramos que, como de todos modos se deben recorrer todas las palabras del texto, es más rentable ir filtrando las palabras a medida que se recorre el mismo en memoria, que agregar otra fase de mensajería por red.

## Diagrama de Robustez
<img width="1964" height="1115" alt="Diagrama de Robustez - TP Escalabilidad" src="https://github.com/user-attachments/assets/efcce454-c3e8-4be6-b86f-2ff9b04ba11e" />

#### Modificaciones / Nuevas decisiones de diseño
* Previamente, para resolver la query 5, la lógica de calcular el promedio y guardar las cantidades de palabras de los demás artículos estaba en un sólo nodo. Tras considerar que esto posiblemente se convierta en un cuello de botella, con mucho consumo de memoria, se separaron las responsabilidades:
   * Un nodo `YearSplitter` es un nodo stateless que se fija si el año es <= ó > a 2020, y en base a ésto envía la data al nodo `OldArticlesAverager` ó `AboveAverageFilter` respectivamente. El envío se realiza a través de un exchange, haciendo uso de hashing por client ID.
   * `OldArticlesAverager` recibe la cantidad de palabras de los artículos <= 2020 para un mismo cliente, y las usa para calcular el promedio hasta que recibe un EOF.
   * `AboveAverageFilter` recibe la cantidad de palabras de los artículos >2020 para un mismo cliente, y las guarda hasta que recibe un EOF.
* **Sincronización multi-upstream y barrera de EOFs (Queries 1 a 5):** Al haber etapas replicadas en múltiples instancias, los workers aguas abajo (`year_filter`, `english_filter`, `bio_filter`, `image_downloader`, `shared_words_aggregator`, `partial_top_aggregator`, `year_splitter` y los `result_aggregator`) implementan una barrera de sincronización por cliente (`recvEofCountMap[clientID] >= UPSTREAM_AMOUNT`). Ningún worker emite resultados parciales ni EOF aguas abajo hasta que todas las réplicas upstream hayan finalizado.
* **Manejo de estado concurrente en Q3 y Q4:** En `SharedWordsAggregator` (acumulación de idiomas `en`/`fr` por palabra) y `PartialTopAggregator` (conteo de frecuencias de palabras), el estado se particiona en mapas aislados por `clientID` protegidos mediante `sync.Mutex`, garantizando la atención simultánea de múltiples clientes sin colisiones ni memory leaks (el estado de un cliente se libera inmediatamente al emitir su resultado).
* **Garantía de orden y no-adelantamiento de EOF:** El enrutamiento por hash determinístico envía los datos de un artículo/palabra y el EOF correspondiente a través de la misma cola directa hacia el mismo shard. Gracias al orden FIFO garantizado por RabbitMQ a nivel de cola, es imposible que un EOF adelante a los datos en tránsito.
* **Thread-Safety en el Middleware AMQP:** Dado que los canales de RabbitMQ (`amqp.Channel`) no son seguros para concurrencia y el Gateway maneja clientes en goroutines independientes, se implementó exclusión mutua (`sendMu sync.Mutex`) en las primitivas de publicación (`Publish`) para evitar la corrupción de frames binarios TCP durante la transmisión simultánea de múltiples clientes.

## Diagrama de Despliegue
<img width="1632" height="552" alt="Diagrama de Despliegue - TP Escalabilidad" src="https://github.com/user-attachments/assets/a3a6f5d2-ee41-4bc3-a634-9019a60b6a73" />

#### Modificaciones / Nuevas decisiones de diseño y Escalabilidad
* **Cumplimiento del requisito de escalabilidad ($\ge 60\%$):** El pipeline cuenta con 16 tipos de workers, de los cuales **10 escalan horizontalmente (62.5%)**:
   * *Nodos compartidos escalados (3):* `ColumnFilter` (2 réplicas), `YearFilter` (2 réplicas), `VocabularyExtractor` (2 réplicas).
   * *Nodos específicos por query escalados (7):* `EnglishFilter` (Q1, 2 réplicas), `BioFilter` (Q2, 2 réplicas), `ImageDownloader` (Q2, 2 réplicas), `SharedWordsAggregator` (Q3, 2 réplicas), `PartialTopAggregator` (Q4, 2 réplicas), `OldArticlesAverager` (Q5, 2 réplicas), `AboveAverageFilter` (Q5, 2 réplicas).
* **Mecanismos de distribución de carga (Sharding):**
   * *Por `article_id`:* Dispersión uniforme mediante `FNV-1a(article_id) % N` para ingesta y filtros de Q1 y Q2.
   * *Por palabra (`word`):* Dispersión mediante `FNV-1a(word) % N` en Q3 y Q4. Garantiza que todas las ocurrencias de una misma palabra caigan en el mismo shard, permitiendo conteos y detecciones de idiomas independientes en paralelo.
   * *Por `client_id`:* Dispersión en Q5 (`OldArticlesAverager` y `AboveAverageFilter`) para balancear la carga de clientes concurrentes.
* **Justificación de workers monoinstancia (6 tipos / 37.5%):**
   * Los sumideros finales (`q1_aggregator` a `q5_aggregator`) actúan como reducers finales de bajo cómputo (ordenamiento alfabético, consolidación de tops parciales o empaquetado del paquete único para el cliente). Replicarlos agregaría sobrecosto de coordinación sin beneficio de throughput.
   * `YearSplitter` realiza un ruteo condicional liviano por año sin retención de estado en memoria.
* **Infraestructura desplegada:** El clúster estándar se compone de **28 contenedores de servicio** (20 réplicas de workers escalados + 6 workers monoinstancia + 1 Gateway TCP↔AMQP + 1 broker RabbitMQ), además de los contenedores de clientes concurrentes.
