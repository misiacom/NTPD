Wydział Telekomunikacji,  
Informatyki i Elektrotechniki  
Nowoczesne Technologie Przetwarzania Danych  
Laboratorium 11  
Temat: Przetwarzanie strumieniowe danych w Apache Spark Structured Streaming

Wiktoria Cendrowska-Kociuga Gr3

Zadanie 1
wersja sparka
4.1.2

Zadanie 2
Po przeciągnięciu pliku events1.csv:
-------------------------------------------
Batch: 0
-------------------------------------------
+-------------------+-------+-----------+------+---------+
|         event_time|user_id|   category|amount|   status|
+-------------------+-------+-----------+------+---------+
|2026-05-01 10:00:00|   u001|      books| 39.99|     paid|
|2026-05-01 10:01:00|   u002|electronics| 120.5|     paid|
|2026-05-01 10:02:00|   u003|       food|  15.2|  pending|
|2026-05-01 10:03:00|   u004|      books| 22.99|     paid|
|2026-05-01 10:04:00|   u005|    clothes|  75.0|cancelled|
+-------------------+-------+-----------+------+---------+

Zadanie 3
po dodaniu do folderu input_stream pliku events2.csv:

-------------------------------------------
Batch: 1
-------------------------------------------
+-----------+------------+------------------+
|   category|events_count|      total_amount|
+-----------+------------+------------------+
|    clothes|           1|              80.0|
|      books|           4|142.98000000000002|
|electronics|           2|             320.5|
+-----------+------------+------------------+
po włączeniu generator.py:
-------------------------------------------
Batch: 2
-------------------------------------------
+-----------+------------+------------+
|   category|events_count|total_amount|
+-----------+------------+------------+
|       food|           1|      146.98|
|    clothes|           1|        80.0|
|      books|           5|      229.25|
|electronics|           3|      403.35|
+-----------+------------+------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----------+------------+------------+
|   category|events_count|total_amount|
+-----------+------------+------------+
|       food|           1|      146.98|
|    clothes|           2|       94.37|
|      books|           5|      229.25|
|electronics|           3|      403.35|
+-----------+------------+------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+-----------+------------+------------------+
|   category|events_count|      total_amount|
+-----------+------------+------------------+
|       food|           2|180.02999999999997|
|    clothes|           2|             94.37|
|      books|           5|            229.25|
|electronics|           5|            721.88|
+-----------+------------+------------------+

zadanie 4
po dodaniu do input_stream pliku events3:
-------------------------------------------
Batch: 2
-------------------------------------------
+------+--------+------------+------------+
|window|category|events_count|total_amount|
+------+--------+------------+------------+
+------+--------+------------+------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+--------+------------+------------+
|window|category|events_count|total_amount|
+------+--------+------------+------------+
+------+--------+------------+------------+

Po dodaniu events_late.csv:
26/06/07 20:22:20 WARN FileStreamSink: Assume no metadata directory. Error while looking for metadata directory in the path: file:/home/wiktoria/PycharmProjects/SparkLab11/data/input_stream/events_late.csv~.
java.io.FileNotFoundException: File file:/home/wiktoria/PycharmProjects/SparkLab11/data/input_stream/events_late.csv~ does not exist

Zadanie 5
Po dodaniu pliku events5.csv do input_stream:
-------------------------------------------
Batch: 1
-------------------------------------------
+-----------+------------+------------------+
|   category|events_count|      total_amount|
+-----------+------------+------------------+
|       food|           3|200.02999999999997|
|    clothes|           2|             94.37|
|      books|          11|           1238.32|
|electronics|           7|            971.88|
+-----------+------------+------------------+ 
wyniki z parquet
+-------------------+-------+-----------+------+------+
|event_time         |user_id|category   |amount|status|
+-------------------+-------+-----------+------+------+
|2026-06-07 20:15:44|u115   |electronics|208.35|paid  |
|2026-06-07 20:15:44|u117   |food       |33.05 |paid  |
|2026-06-07 20:15:44|u119   |electronics|110.18|paid  |
|2026-05-01 11:00:00|u006   |books      |50.0  |paid  |
|2026-05-01 11:02:00|u007   |electronics|200.0 |paid  |
|2026-05-01 11:06:00|u009   |books      |30.0  |paid  |
|2026-05-01 11:08:00|u010   |clothes    |80.0  |paid  |
|2026-05-01 10:01:00|u101   |books      |50.0  |paid  |
|2026-05-01 10:03:00|u102   |electronics|100.0 |paid  |
|2026-05-01 10:06:00|u103   |food       |20.0  |paid  |
|2026-05-01 10:08:00|u104   |books      |80.0  |paid  |
|2026-05-01 10:00:00|u001   |books      |39.99 |paid  |
|2026-05-01 10:01:00|u002   |electronics|120.5 |paid  |
|2026-05-01 10:03:00|u004   |books      |22.99 |paid  |
|2026-05-01 12:00:00|u201   |books      |100.0 |paid  |
|2026-05-01 12:01:00|u202   |electronics|150.0 |paid  |
|2026-05-01 12:03:00|u204   |books      |80.0  |paid  |
|2026-06-07 20:15:29|u101   |electronics|82.85 |paid  |
|2026-06-07 20:15:29|u102   |food       |146.98|paid  |
|2026-06-07 20:15:39|u112   |clothes    |14.37 |paid  |
+-------------------+-------+-----------+------+------+
only showing top 20 rows

**Przetwarzanie batch a streaming**

Przetwarzanie batch polega na analizie zamkniętego zbioru danych w jednym uruchomieniu programu. Dane są wcześniej zapisane i przetwarzane jednorazowo. Streaming natomiast umożliwia przetwarzanie danych napływających na bieżąco, bez konieczności restartowania aplikacji.

**Tryby append, update i complete**

Tryb **append** zapisuje wyłącznie nowe dane, które pojawiły się w strumieniu. Tryb **update** aktualizuje tylko rekordy zmienione od poprzedniej mikro-partii, natomiast **complete** wyświetla lub zapisuje pełny wynik agregacji za każdym razem. W laboratorium wykorzystano `complete` do konsoli i `append` przy zapisie danych do Parquet.

**Checkpointing a zwykły zapis plików wynikowych**

Checkpointing zapisuje stan przetwarzania strumieniowego i informację o już obsłużonych danych, dzięki czemu po restarcie aplikacja nie przetwarza ich ponownie. Zwykły zapis plików wynikowych przechowuje jedynie dane wyjściowe, ale nie pamięta postępu przetwarzania. Dzięki checkpointingowi aplikacja jest bardziej odporna na restart lub awarię.
