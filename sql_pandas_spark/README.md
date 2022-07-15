### Вопросы по pandas, spark, sql

<details>
<summary>Что такое соленье датафрейма?</summary>
<div> <br />
	Соление датафрейма - процесс оптимизации работы спарка с партициями путём добавления агрегатора, который будет дополнительно разбивать наши партиции на более мелкие.
    
Для чего это нужно?

1) Стабилизирует работу ЕТЛ процесса
2) Ускоряет скорость обработки датафрейма (не всегда)

Минусы:
1) Нужно дополлнительно придумывать, как обрабатывать датафрейм с новыми группировками


Пример:
    ```
    airports.printSchema
    ```

    airports.groupBy('type).count.orderBy('count.desc).show
    root
     |-- ident: string (nullable = true)
     |-- type: string (nullable = true)
     |-- name: string (nullable = true)
     |-- elevation_ft: integer (nullable = true)
     |-- continent: string (nullable = true)
     |-- iso_country: string (nullable = true)
     |-- iso_region: string (nullable = true)
     |-- municipality: string (nullable = true)
     |-- gps_code: string (nullable = true)
     |-- iata_code: string (nullable = true)
     |-- local_code: string (nullable = true)
     |-- coordinates: string (nullable = true)
    
    +--------------+-----+
    |          type|count|
    +--------------+-----+
    | small_airport|34369|
    |      heliport|11507|
    |medium_airport| 4537|
    |        closed| 4154|
    | seaplane_base| 1020|
    | large_airport|  616|
    |   balloonport|   23|
    +--------------+-----+

Если применить соленья, например разбить на 10 партиций

    val saltModTen = pmod(round((rand() * 100), 0), lit(10)).cast("int")
    val salted = airports.withColumn("salt", saltModTen)
    salted.show(numRows = 1, truncate = 200, vertical = true)

Тогда мы получим размер партиций, уменьшенный в 10 раз. 

    val firstStep = salted.groupBy('type, 'salt).count()
    
    firstStep.orderBy('count.desc).show(200, false)
    +--------------+----+-----+
    |type          |salt|count|
    +--------------+----+-----+
    |small_airport |9   |3483 |
    |small_airport |4   |3472 |
    |small_airport |8   |3468 |
    |small_airport |6   |3463 |
    |small_airport |7   |3453 |
    |small_airport |1   |3439 |
    |small_airport |3   |3419 |
    |small_airport |5   |3411 |
    |small_airport |2   |3381 |
    |small_airport |0   |3380 |
    |heliport      |8   |1179 |
    |heliport      |2   |1179 |
    |heliport      |0   |1172 |
    |heliport      |9   |1157 |
    |heliport      |3   |1152 |
    |heliport      |4   |1148 |
    |heliport      |6   |1143 |
    |heliport      |5   |1142 |
    |heliport      |7   |1140 |
    |heliport      |1   |1095 |
    |medium_airport|4   |493  |
    |medium_airport|0   |474  |
    |medium_airport|8   |448  |
    |medium_airport|6   |446  |
    |medium_airport|3   |446  |
    |closed        |8   |440  |
    |medium_airport|9   |438  |
    |closed        |4   |436  |
    |closed        |3   |435  |
    |medium_airport|1   |431  |
    |medium_airport|2   |427  |
    |closed        |0   |421  |
    |closed        |6   |411  |
    |closed        |7   |400  |
    |closed        |9   |392  |
    |closed        |5   |388  |
    |seaplane_base |3   |112  |
    |seaplane_base |7   |109  |
    |seaplane_base |5   |107  |
    |large_airport |1   |63   |
    |large_airport |3   |62   |
    |large_airport |4   |60   |
    |large_airport |2   |59   |
    |large_airport |6   |59   |
    |large_airport |0   |58   |
    |large_airport |5   |55   |
    |large_airport |9   |55   |
    |balloonport   |6   |2    |
    |balloonport   |5   |2    |
    |balloonport   |1   |2    |
    |balloonport   |3   |1    |
    |balloonport   |7   |1    |
    +--------------+----+-----+


	<p></p>
	<b></b>

</div>
</details>