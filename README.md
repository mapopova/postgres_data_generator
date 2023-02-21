# Генератор синтетических данных

Процедуры для генерации синтетических таблиц и связей между ними.

**generate_table_id_inc(tab_name, col_name, min_num, max_num)** \
Генерирует ряд уникальных инкрементальных идентификаторов от *min_num* до *max_num*.

Параметры:
* **tab_name** - имя генерируемой таблицы
* **col_name** - имя генерируемого столбца
* **min_num** - минимальное значение идентификатора
* **max_num** - максимальное значение идентификатора

Пример:
```
CALL generate_table_id_inc(
    tab_name=>'inc', col_name=>'id',
    min_num=>100, max_num=>500) 
```

&nbsp;

**generate_table_id_random(tab_name, col_name, min_num, max_num, amount)** \
Генерирует ряд из *amount* уникальных рандомных идентификаторов в промежутке от *min_num* до *max_num*. *amount* не должно быть больше, чем размер задаваемого промежутка.

Параметры:
* **tab_name** - имя генерируемой таблицы
* **col_name** - имя генерируемого столбца
* **min_num** - нижняя граница промежутка
* **max_num** - вехняя граница промежутка
* **amount** - количество идентификаторов

&nbsp;

**generate_table_uuid(tab_name, col_name, amount)** \
Генерирует ряд из *amount* уникальных рандомных uuid.

Параметры:
* **tab_name** - имя генерируемой таблицы
* **col_name** - имя генерируемого столбца
* **amount** - количество идентификаторов


&nbsp;

**generate_one_to_one_pairs(tab_one, col_one, tab_two, col_two, tab_target, amount)** \
Генерирует *amount* рандомных уникальных пар ключей, при условии, что каждому ключу из *tab_one* соответсвует один и только один ключ из *tab_two* (связь 1:1). *amount* не должно быть больше, чем количество строк в меньшей из таблиц *tab_one*, *tab_two*.

Параметры:
* **tab_one** - имя первой таблицы ключей
* **col_one** - имя столбца в первой таблице ключей
* **tab_two** - имя второй таблицы ключей
* **col_two** - имя столбца во второй таблице ключей
* **tab_target** - имя генерируемой таблицы
* **amount** - количество генерируемых пар

&nbsp;

**generate_one_to_many_pairs(tab_one, col_one, tab_two, col_two, tab_target, amount)** \
Генерирует *amount* рандомных уникальных пар ключей, при условии, что каждому ключу из *tab_one* соответсвует ноль или более ключей из *tab_two*, при этом каждому ключу из *tab_two* соответствует один и только один ключ из *tab_one* (связь 1:0..N). *amount* не должно быть больше, чем количество строк в таблице *tab_two*.

Параметры:
* **tab_one** - имя первой таблицы ключей
* **col_one** - имя столбца в первой таблице ключей
* **tab_two** - имя второй таблицы ключей
* **col_two** - имя столбца во второй таблице ключей
* **tab_target** - имя генерируемой таблицы
* **amount** - количество генерируемых пар

&nbsp;

**generate_one_to_many_pairs_v2(tab_one, col_one, tab_two, col_two, tab_target, amount)** \
Генерирует *amount* рандомных уникальных пар ключей, при условии, что каждому ключу из *tab_one* соответсвует один или более ключей из *tab_two*, при этом каждому ключу из *tab_two* соответствует один и только один ключ из *tab_one* (связь 1:1..N). Количество строк в таблице *tab_two* должно быть больше, чем количество строк в таблице *tab_one*.

Параметры:
* **tab_one** - имя первой таблицы ключей
* **col_one** - имя столбца в первой таблице ключей
* **tab_two** - имя второй таблицы ключей
* **col_two** - имя столбца во второй таблице ключей
* **tab_target** - имя генерируемой таблицы
* **amount** - количество генерируемых пар

