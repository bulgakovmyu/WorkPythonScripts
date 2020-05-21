import requests
import pandas as pd
import re
import time
import os
from termcolor import colored

overpass_url = "http://overpass-api.de/api/interpreter"
output_catalogue = os.getcwd()
print(colored('Output Catalogue is '+output_catalogue,'yellow'))


# Функция, которая проверяет доступность сервера для запроса и ждет когда он будет доступен.
# В API есть ограничение на количество запросов их периодичность, поэтому чтобы не напороться на обшибку необходимо подождать доступности
def waiting_for_request():
    overpass_test_url = "http://overpass-api.de/api/status"
    available_test_request = None
    i = 0
    while available_test_request is None:
        available_test_request = requests.get(overpass_test_url).text
        available_test_request = re.search(r'(\d+)\sslots\savailable', available_test_request)
        if (available_test_request is None):
            #             print('No free attempts for query. Waiting 10 seconds for next try.')
            time.sleep(10)
            i += 1
    #     print(available_test_request.group(1) + ' slots are available')
    return True


# Функция выгрузки справочника стран
def get_countries():
    waiting_for_request()
    # Запрос на выгрузку отношений стран
    overpass_query1 = """
            [out:json];relation["admin_level"="2"];out;
            """
    response1 = requests.get(overpass_url,
                             params={'data': overpass_query1})
    data1 = response1.json()

    waiting_for_request()
    # Запрос  на выгрузку точек стран
    overpass_query2 = """
            [out:json];node[place=country];out;
            """
    response2 = requests.get(overpass_url,
                             params={'data': overpass_query2})
    data2 = response2.json()

    # Забираем атрибуты стран из выгрузки отношений
    arr1 = []
    for element in data1['elements']:
        if ('name:ru' in element['tags']) and ("ISO3166-1:alpha2" in element['tags']):
            if ('short_name:ru' in element['tags']):
                arr1.append({'Страна': element['tags']['name:ru'],
                             'Страна_short': element['tags']['short_name:ru'],
                             'Страна_en': element['tags']['name:en'],
                             'alpha2': element['tags']["ISO3166-1:alpha2"]})
            else:
                arr1.append({'Страна': element['tags']['name:ru'],
                             'Страна_short': element['tags']['name:ru'],
                             'Страна_en': element['tags']['name:en'],
                             'alpha2': element['tags']["ISO3166-1:alpha2"]})
    # Забираем атрибуты стран из выгрузки точек
    arr2 = []
    for element in data2['elements']:
        if ('name:ru' in element['tags']) and ("country_code_iso3166_1_alpha_2" in element['tags']):
            if ('short_name:ru' in element['tags']):
                arr2.append({'Страна': element['tags']['name:ru'],
                             'Страна_short': element['tags']['short_name:ru'],
                             'Страна_en': element['tags']['name:en'],
                             'alpha2': element['tags']["country_code_iso3166_1_alpha_2"]})
            else:
                arr2.append({'Страна': element['tags']['name:ru'],
                             'Страна_short': element['tags']['name:ru'],
                             'Страна_en': element['tags']['name:en'],
                             'alpha2': element['tags']["country_code_iso3166_1_alpha_2"]})
    DF = pd.concat([pd.DataFrame(arr1), pd.DataFrame(arr2)])
    DF = DF.drop_duplicates('alpha2').reset_index(drop=True)

    # Выгружаем таблицу стран в csv
    DF.to_csv(output_catalogue + '\\Countries_vocabulary.csv', index=False, encoding='utf-8-sig')
    return DF


# Функция выгрузки справочника федеральных округов
def get_federal_districts():
    waiting_for_request()
    overpass_query_FD = """
            [out:json];relation['addr:country'=RU][admin_level=3];out;
            """
    response_FD = requests.get(overpass_url,
                               params={'data': overpass_query_FD})
    data_FD = response_FD.json()

    arr_FD = []
    for element in data_FD['elements']:
        for ID in [i['ref'] for i in element['members'] if i['type'] == 'relation']:
            if ('name:ru' in element['tags']) and ("addr:country" in element['tags']):
                arr_FD.append({'Страна': element['tags']["addr:country"],
                               'Федеральный округ': element['tags']['name:ru'],
                               'Inner_region_ID': ID})
    DF_FD = pd.DataFrame(arr_FD)
    DF_FD[['Страна', 'Федеральный округ']].drop_duplicates().to_csv(output_catalogue+'\\Federal_districts_vocabulary.csv', index=False, encoding="windows-1251")
    return DF_FD


# Функция выгрузки справочника регионов РФ
def get_regions(Fed_district_DF):
    regions_id = 'relation(' + ');relation('.join(Fed_district_DF.Inner_region_ID.astype(str).to_list()) + ');'
    waiting_for_request()
    overpass_query_Reg = """
            [out:json];
            (regions_id
            );
            out;
            """.replace('regions_id', regions_id)
    response_Reg = requests.get(overpass_url,
                                params={'data': overpass_query_Reg})
    data_Reg = response_Reg.json()
    Regions_arr = [{'Inner_region_ID': i['id'], 'Регион': re.sub(r'\s—.+','',i['tags']['name'])} for i in data_Reg['elements']]
    Regions_DF = pd.DataFrame(Regions_arr).drop_duplicates()
    Regions_DF = pd.merge(Fed_district_DF, Regions_DF, how='left', left_on='Inner_region_ID',
                          right_on='Inner_region_ID').drop(columns=['Inner_region_ID'])

    ###Добавляем крым в список регионов РФ.
    Regions_DF = Regions_DF.append(pd.DataFrame([{'Страна': 'RU',
                                    'Федеральный округ': 'Южный федеральный округ',
                                    'Регион': 'Республика Крым'}]),ignore_index =True)

    # Проверка однозначности соответствия региона и федерального округа
    assert ((Regions_DF.groupby(Regions_DF.Регион)
             .count()
             .sort_values('Федеральный округ', ascending=False)
             .sum())[0]
            ==
            (Regions_DF
            .groupby(Regions_DF.Регион)
            .count().sort_values('Федеральный округ', ascending=False)
            .shape[0])) == True, 'Однозначность соответствия региона и Федерально округа не соблюдена'

    # Выгрузка регионов в csv
    Regions_DF.to_csv(output_catalogue + '\\Regions_vocabulary.csv', index=False, encoding="windows-1251")

    # Проверяем количество регионов в федеральных округах
    if ((pd.DataFrame([{'Федеральный округ': 'Дальневосточный федеральный округ', 'Количество': 11},
                    {'Федеральный округ': 'Приволжский федеральный округ', 'Количество': 14},
                    {'Федеральный округ': 'Северо-Западный федеральный округ', 'Количество': 11},
                    {'Федеральный округ': 'Северо-Кавказский федеральный округ', 'Количество': 7},
                    {'Федеральный округ': 'Сибирский федеральный округ', 'Количество': 10},
                    {'Федеральный округ': 'Уральский федеральный округ', 'Количество': 6},
                    {'Федеральный округ': 'Центральный федеральный округ', 'Количество': 18},
                    {'Федеральный округ': 'Южный федеральный округ', 'Количество': 8}, ]).sort_values('Количество', ascending=False).Количество.to_list())
        ==
        (Regions_DF.groupby(Regions_DF['Федеральный округ']).count().sort_values('Регион', ascending=False)).Регион.to_list()) is True:
        print(colored('Number of regions in federal district is correct', 'green'))
    else:
        print(colored('Need to check number of regions in federal district', 'yellow'))

    return Regions_DF


# Функция выгрузки справочника городов
def get_cities():
    # убираем ненужные предупреждения
    pd.options.mode.chained_assignment = None  # default='warn'
    waiting_for_request()

    ### Запрос для выгрузки городов
    #     Закомментированный текст запроса, чтобы добавить в список маленькие города, если вдруг сильно будет нужно.
    #     overpass_query_Cities = """
    #             [out:json];relation['addr:country'=RU]["place"~"city|town"];out;
    #             """
    overpass_query_Cities = """
            [out:json];relation['addr:country'=RU]["place"~"city"];out;
            """
    response_Cities = requests.get(overpass_url,
                                   params={'data': overpass_query_Cities})
    data_Cities = response_Cities.json()
    arr_Cities = []

    # Выгрузка атрибутов городов
    for element in data_Cities['elements']:
        if ('name:ru' in element['tags']) & ('addr:region' in element['tags']):
            if element['tags']['name:ru'] != 'イシリクリ':
                arr_Cities.append({'Регион': element['tags']['addr:region'],
                                   'Город': element['tags']['name:ru']})
            else:
                arr_Cities.append({'Регион': element['tags']['addr:region'],
                                   'Город': element['tags']['name']})
        elif ('name' in element['tags']) & ('addr:region' in element['tags']):
            arr_Cities.append({'Регион': element['tags']['addr:region'],
                               'Город': element['tags']['name']})

    #     создаем технические столбцы для сравнений
    Cities_DF = pd.DataFrame(arr_Cities)
    Cities_DF['Var1'] = Cities_DF['Регион']
    Cities_DF['Var2'] = Cities_DF['Регион']
    Cities_DF['Регион_test'] = Cities_DF['Регион'].isin(list(Regions_DF['Регион']))

    # Проверяем все ли города сопоставлены своим регионам и если нет - делаем преобразования чтобы сопоставить:
    not_found = list(Cities_DF[Cities_DF.Регион_test == False].Регион)

    var1 = []
    var2 = []
    for el in not_found:
        if re.search(r'Республика\s(.+)', el) is None:
            var1.append('Республика ' + el)
            var2.append(None)
        else:
            var1.append(None)
            var2.append(re.search(r'Республика\s(.+)', el).group(1))

    Cities_DF.Var1[Cities_DF.Регион_test == False] = var1
    Cities_DF.Var2[Cities_DF.Регион_test == False] = var2

    Cities_DF['Регион_test1'] = Cities_DF['Var1'].isin(list(Regions_DF['Регион']))
    Cities_DF['Регион_test2'] = Cities_DF['Var2'].isin(list(Regions_DF['Регион']))

    LF = lambda row: row['Регион'] if row['Регион_test'] == True else (row['Var1'] if row['Регион_test1'] == True
                                                                       else (
        row['Var2'] if row['Регион_test2'] == True else None))

    Cities_DF['Регион'] = Cities_DF.apply(LF, axis=1)
    Cities_DF['Регион_test'] = Cities_DF['Регион'].isin(list(Regions_DF['Регион']))
    if Cities_DF[Cities_DF.Регион_test == False].shape[0] == 0:
        print('All cities are mapped')
        Cities_DF = Cities_DF.drop(columns=['Var1', 'Var2', 'Регион_test', 'Регион_test1', 'Регион_test2'])

    DF = pd.merge(Cities_DF, Regions_DF, how='left').drop_duplicates()[['Город', 'Регион', 'Федеральный округ']]

    DF.to_csv(output_catalogue + '\\Cities_vocabulary.csv', index=False, encoding="windows-1251")

    return DF


DF_countries = get_countries()
time.sleep(2)
print(DF_countries.head(3))
print(colored('Countries are downloaded successfully!', 'green'))

Fed_district_DF = get_federal_districts()
time.sleep(2)
print(Fed_district_DF.head(2))
print(colored('Federal districts are downloaded successfully!', 'green'))

Regions_DF = get_regions(Fed_district_DF)
time.sleep(2)
print(Regions_DF.head(3))
print(colored('Regions are downloaded successfully!', 'green'))

Cities_DF = get_cities()
time.sleep(2)
print(Cities_DF.head(3))
print(colored('Cities districts are downloaded successfully!','green'))