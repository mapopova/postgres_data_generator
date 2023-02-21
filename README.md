# Заголовок

*описание*

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
Генерирует ряд из *amount* уникальных рандомных идентификаторов в промежутке от *min_num* до *max_num*.

Параметры:
* **tab_name** - имя генерируемой таблицы
* **col_name** - имя генерируемого столбца
* **min_num** - нижняя граница промежутка
* **max_num** - вехняя граница промежутка
* **amount** - количество идентификаторов

&nbsp;

**generate_table_uuid(tab_name, col_name, amount)** \
Генерирует ряд из *amount* уникальных рандомных uuid.

&nbsp;

**generate_one_to_one_pairs(tab_one, col_one, tab_two, col_two, tab_target, amount)** \
Генерирует *amount* рандомных пар ключей, при условии, что каждому ключу из *tab_one* соответсвует один и только один ключ из *tab_two*. amount должно быть <= min(dim A, dim B)

&nbsp;

**generate_one_to_many_pairs(tab_one, col_one, tab_two, col_two, tab_target, amount)** \ 
Генерирует *amount* рандомных пар ключей, при условии, что каждому ключу из *tab_one* соответсвует ноль или более ключей из *tab_two*.

