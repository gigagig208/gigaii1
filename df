import pandas as pd

df_plan = pd.read_csv("План.csv")
df_plan

import psycopg2,csv

conntion = psycopg2.connect(database="tourism2", user="univer", password="univer", port=5432, host="127.0.0.1")
cur = conntion.cursor()

with open("План.csv", "r", encoding="utf-8") as file:
    reader = csv.reader(file)
    data = list(reader)
    data.pop(0)

sql_create_table_plan = """
CREATE TABLE public."план" (
    "Субъект РФ" text,
    "План Октябрь 2020 г" integer,
    "План Ноябрь 2020 г" integer,
    "План Декабрь 2020 г" integer,
    "План Январь 2021 г" integer,
    "План Февраль 2021 г" integer,
    "План Март 2021 г" integer,
    "План Апрель 2021 г" integer,
    "План Май 2021 г" integer,
    "План Июнь 2021 г" integer,
    "План Июль 2021 г" integer,
    "План Август 2021 г" integer,
    "План Сентябрь 2021 г" integer,
    "План Октябрь 2021 г" integer,
    "План Ноябрь 2021 г" integer,
    "План Декабрь 2021 г" integer,
    "План 2021 г" integer,
    "План Январь 2022 г" integer,
    "План Февраль 2022 г" integer,
    "План Март 2022 г" integer,
    "План Апрель 2022 г" integer,
    "План Май 2022 г" integer,
    "План Июнь 2022 г" integer,
    "План Июль 2022 г" integer,
    "План Август 2022 г" integer,
    "План Сентябрь 2022 г" integer,
    "План Октябрь 2022 г" integer
);
"""

cur.execute(sql_create_table_plan)
conntion.commit()

for i in range(len(data)):
    sql_insert_plan = f"""
    INSERT INTO public."план" (
        "Субъект РФ", 
        "План Октябрь 2020 г",
        "План Ноябрь 2020 г",
        "План Декабрь 2020 г",
        "План Январь 2021 г",
        "План Февраль 2021 г",
        "План Март 2021 г",
        "План Апрель 2021 г",
        "План Май 2021 г",
        "План Июнь 2021 г",
        "План Июль 2021 г",
        "План Август 2021 г",
        "План Сентябрь 2021 г",
        "План Октябрь 2021 г",
        "План Ноябрь 2021 г",
        "План Декабрь 2021 г",
        "План 2021 г",
        "План Январь 2022 г",
        "План Февраль 2022 г",
        "План Март 2022 г",
        "План Апрель 2022 г",
        "План Май 2022 г",
        "План Июнь 2022 г",
        "План Июль 2022 г",
        "План Август 2022 г",
        "План Сентябрь 2022 г",
        "План Октябрь 2022 г")  
    VALUES (
        '{data[i][0]}',
        {data[i][1]},
        {data[i][2]},
        {data[i][3]},
        {data[i][4]},
        {data[i][5]},
        {data[i][6]},
        {data[i][7]},
        {data[i][8]},
        {data[i][9]},
        {data[i][10]},
        {data[i][11]},
        {data[i][12]},
        {data[i][13]},
        {data[i][14]},
        {data[i][15]},
        {data[i][16]},
        {data[i][17]},
        {data[i][18]},
        {data[i][19]},
        {data[i][20]},
        {data[i][21]},
        {data[i][22]},
        {data[i][23]},
        {data[i][24]},
        {data[i][25]},
        {data[i][26]}
        );

    """

    cur.execute(sql_insert_plan)
    conntion.commit()
    
conntion.close()

text_in_cp1251 = open('Общая статистика.csv', 'rb').read()
text_in_unicode = text_in_cp1251.decode('cp1251')
text_in_utf8 = text_in_unicode.encode('utf8')
open('stat.csv', 'wb').write(text_in_utf8)


import pandas as pd

df_stat = pd.read_csv("stat.csv",delimiter=";")
df_stat

df_stat.dropna(inplace=True)
df_stat
df_stat.to_csv("stat2.csv", index=False)
import csv
import psycopg2


with open("stat2.csv", "r", encoding="utf-8") as file:
    reader = csv.reader(file)
    data = list(reader)
    data.pop(0)

conn = psycopg2.connect(database="tourism2", user="univer", password="univer", port=5432, host="127.0.0.1")
cur = conn.cursor()

sql_create_ststic = """
CREATE TABLE public."общая статистика" (
    "Регион" text,
    "Центр региона" text,
    "Федеральный округ" text,
    "Общее количество достопримечател" text,
    "Активный отдых" integer,
    "Воинская слава" integer,
    "Гастрономический туризм" integer,
    "Детский отдых" integer,
    "Культура" integer,
    "Музеи" integer,
    "Необычные места" integer,
    "Оздоровительный туризм" integer,
    "Охота и рыбалка" integer,
    "Пляжный отдых" integer,
    "Приключения" integer,
    "Природа" integer,
    "Развлечения" integer,
    "Святыни и храмы" integer,
    "Сельский отдых" integer,
    "Театры" integer,
    "Традиции" integer,
    "Временная разница с Москвой" integer,
    "Численность населения" integer
    );
    
"""
cur.execute(sql_create_ststic)
conn.commit()


for i in range(len(data)):
    sql_insert_static = f"""
    INSERT INTO public."общая статистика" (
    "Регион",
    "Центр региона",
    "Федеральный округ",
    "Общее количество достопримечател",
    "Активный отдых",
    "Воинская слава",
    "Гастрономический туризм",
    "Детский отдых",
    "Культура",
    "Музеи",
    "Необычные места",
    "Оздоровительный туризм",
    "Охота и рыбалка",
    "Пляжный отдых",
    "Приключения",
    "Природа",
    "Развлечения",
    "Святыни и храмы",
    "Сельский отдых",
    "Театры",
    "Традиции",
    "Временная разница с Москвой",
    "Численность населения") 
VALUES (
    '{data[i][0]}',
    '{data[i][1]}',
    '{data[i][2]}',
    '{data[i][3]}',
    {data[i][4]},
    {data[i][5]},
    {data[i][6]},
    {data[i][7]},
    {data[i][8]},
    {data[i][9]},
    {data[i][10]},
    {data[i][11]},
    {data[i][12]},
    {data[i][13]},
    {data[i][14]},
    {data[i][15]},
    {data[i][16]},
    {data[i][17]},
    {data[i][18]},
    {data[i][19]},
    {data[i][20]},
    {data[i][21]},
    {data[i][22]}
    );
"""

    cur.execute(sql_insert_static)

conn.commit()
conn.close()



import pandas as pd

df_tourism = pd.read_csv("туризм.csv")
df_tourism


conn = psycopg2.connect(database="tourism2", user="univer", password="univer", host="127.0.0.1", port=5432)
cur = conn.cursor()

sql_create_tourism = """
CREATE TABLE public."туризм" (
    "дата" timestamp,
    "заголовок" text,
    "текст" text,
    "обработанныи_текст_удалены_эмодзи" text,
    "обработанныи_текст_лемматизация_и" text,
    "источник" text,
    "тип_источника" text,
    "тип_сообщения" text,
    "тип_автора" text,
    "место_публикации" text,
    "пол" text,
    "возраст" float NULL,
    "аудитория" integer NULL,
    "комментариев" integer NULL,
    "цитируемость_сми" float NULL,
    "репостов" integer NULL,
    "лаиков" integer NULL,
    "вовлеченность" integer NULL,
    "просмотров" float NULL,
    "дублеи" integer NULL,
    "тональность" text,
    "страна" text,
    "регион" text,
    "город" text,
    "язык" text,
    "место" text,
    "номер_тематического_кластера" integer NULL
);
"""

cur.execute(sql_create_tourism)
conn.commit()
import psycopg2

nRow = 0
th = ['address','name_ru','rating','rubrics','text']
lth = [7, 7, 6, 7, 4]
aa = ['','','','','']
Connection = psycopg2.connect(user = "univer", password = "univer", host = "localhost", port = "5432", database = 'tskv')
CurrentCursor =  Connection.cursor()

sql_create_table = """
CREATE TABLE punlic.tskv (
    address text,
    name_ru text,
    rating integer,
    rubrics text,
    text text
);

"""

CurrentCursor.execute(sql_create_table)
Connection.commit()


with open("geo-reviews-dataset-2023.tskv", "r") as inp:
    i = 0    
    for line in inp:
        line = line.replace("'", '"')
        arr = line.split("\t")
        j2 = 0
        for j1 in range(0, 5):
            ss = arr[j2]
            a = ss[0:lth[j1]+1]
            if a == (th[j1]+'='):
                aa[j1] = ss[lth[j1]+1:]
                j2 += 1
            else:
                aa[j1] = ''
            j1 += 1
        i+=1
        r = aa[2]
        c1 = r[-1]
        if c1 == '.': r = r[:-1]
        aa[0] = aa[0][0:99]
        aa[1] = aa[1][0:99]
        aa[3] = aa[3][0:99]
        aa[4] = aa[4][0:999]
        SQL = f"INSERT INTO public.tskv2(address, name_ru, rating, rubrics, text) VALUES ('{aa[0]}', '{aa[1]}', {r}, '{aa[3]}', '{aa[4]}');"
        CurrentCursor.execute(SQL)
        Connection.commit()
        #if i == 1000: break
        pass
print("end\n")
                
with open("туризм.csv", "r", encoding="utf-8") as file:
    reader = csv.reader(file, delimiter=",")
    header = next(reader)
    data = list(reader) 

sql_insert = """
INSERT INTO public."туризм" (
    "дата",
    "заголовок",
    "текст",
    "обработанныи_текст_удалены_эмодзи",
    "обработанныи_текст_лемматизация_и",
    "источник",
    "тип_источника",
    "тип_сообщения",
    "тип_автора",
    "место_публикации",
    "пол",
    "возраст",
    "аудитория",
    "комментариев",
    "цитируемость_сми",
    "репостов",
    "лаиков",
    "вовлеченность",
    "просмотров",
    "дублеи",
    "тональность",
    "страна",
    "регион",
    "город",
    "язык",
    "место",
    "номер_тематического_кластера"
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

for row in data:
    row[0] = datetime.datetime.strptime(row[0], '%d.%m.%Y %H:%M')
    row[11] = float(row[11]) if row[11] else None  # возраст
    row[12] = int(row[12]) if row[12] else None    # аудитория
    row[13] = int(row[13]) if row[13] else None    # комментариев
    row[14] = float(row[14]) if row[14] else None  # цитируемость_сми
    row[15] = int(row[15]) if row[15] else None    # репостов
    row[16] = int(row[16]) if row[16] else None    # лаиков
    row[17] = int(row[17]) if row[17] else None    # вовлеченность
    row[18] = float(row[18]) if row[18] else None  # просмотров
    row[19] = int(row[19]) if row[19] else None    # дублеи
    row[26] = int(row[26]) if row[26] else None    # номер_тематического_кластера
    cur.execute(sql_insert, row)

conn.commit()
cur.close()
conn.close()

import psycopg2

nRow = 0
th = ['address','name_ru','rating','rubrics','text']
lth = [7, 7, 6, 7, 4]
aa = ['','','','','']
Connection = psycopg2.connect(user = "univer", password = "univer", host = "localhost", port = "5432", database = 'tskv')
CurrentCursor =  Connection.cursor()

sql_create_table = """
CREATE TABLE punlic.tskv (
    address text,
    name_ru text,
    rating integer,
    rubrics text,
    text text
);

"""

CurrentCursor.execute(sql_create_table)
Connection.commit()


with open("geo-reviews-dataset-2023.tskv", "r") as inp:
    i = 0    
    for line in inp:
        line = line.replace("'", '"')
        arr = line.split("\t")
        j2 = 0
        for j1 in range(0, 5):
            ss = arr[j2]
            a = ss[0:lth[j1]+1]
            if a == (th[j1]+'='):
                aa[j1] = ss[lth[j1]+1:]
                j2 += 1
            else:
                aa[j1] = ''
            j1 += 1
        i+=1
        r = aa[2]
        c1 = r[-1]
        if c1 == '.': r = r[:-1]
        aa[0] = aa[0][0:99]
        aa[1] = aa[1][0:99]
        aa[3] = aa[3][0:99]
        aa[4] = aa[4][0:999]
        SQL = f"INSERT INTO public.tskv2(address, name_ru, rating, rubrics, text) VALUES ('{aa[0]}', '{aa[1]}', {r}, '{aa[3]}', '{aa[4]}');"
        CurrentCursor.execute(SQL)
        Connection.commit()
        #if i == 1000: break
        pass
print("end\n")
            
