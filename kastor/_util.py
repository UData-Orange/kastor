######################################################################################
# Copyright (c) 2023 Orange - All Rights Reserved                             #
# * This software is the confidential and proprietary information of Orange.         #
# * You shall not disclose such Restricted Information and shall use it only in      #
#   accordance with the terms of the license agreement you entered into with Orange  #
#   named the "Kastor - Python Library Evaluation License".                          #
# * Unauthorized copying of this file, via any medium is strictly prohibited.        #
# * See the "LICENSE.md" file for more details.                                      #
######################################################################################

from os import path
from khiops import core as kh
from sys import exit
import datetime


def detect_format_timestamp(dictionary, name_dico, name_variable):
    """Détection dans le dictionnaire khiops du format renseigné
    pour un Timestamp ou une Date"""
    dico_domain = kh.read_dictionary_file(dictionary)
    name_dico_flag = False
    name_variable_flag = False
    for dico in dico_domain.dictionaries:
        if dico.name == name_dico :
            name_dico_flag = True
            for var in dico.variables:
                name_variable_flag = True
                if var.name == name_variable:
                    if var.type == "Date":
                        timestamp_type = var.type
                        try:
                            format_timestamp_khiops = var.meta_data.get_value(
                                "DateFormat"
                            )
                            format_timestamp_python = convert_format_date(
                                format_timestamp_khiops
                            )
                            break
                        except KeyError:
                            format_timestamp_python = "%Y-%m-%d"
                            format_timestamp_khiops = "YYYY-MM-DD"

                    elif var.type == "Timestamp":
                        timestamp_type = var.type
                        try:
                            format_timestamp_khiops = var.meta_data.get_value(
                                "TimestampFormat"
                            )
                            format_timestamp_python = convert_format_timestamp(
                                format_timestamp_khiops
                            )
                            break
                        except KeyError:
                            format_timestamp_python = "%Y-%m-%d %H:%M:%S"
                            format_timestamp_khiops = "YYYY-MM-DD HH:MM:SS"

                    elif var.type == "TimestampTZ":
                        timestamp_type = var.type
                        try:
                            format_timestamp_khiops = var.meta_data.get_value(
                                "TimestampTZFormat"
                            )
                            format_timestamp_python = convert_format_timestampTZ(
                                format_timestamp_khiops
                            )
                            break
                        except KeyError:
                            format_timestamp_python = "%Y-%m-%d %H:%M:%S" # rajouter %fZ ou %z
                            format_timestamp_khiops = "YYYY-MM-DD HH:MM:SS.zzzzzz"

                    else:
                        print(
                            "Erreur -> La variable "
                            + name_variable
                            + " doit être de type Date, Timestamp ou TimestampTZ"
                        )
                        exit()
                    break
            break
    if not name_dico_flag:
        print(
            "Erreur -> Le dictionnaire "
            + name_dico
            + " déclaré dans les paramètres n'existe pas dans le dictionnaire "
            + "Khiops "
            + dictionary
        )
        exit()
    if not name_variable_flag:
        print(
            "Erreur -> La variable "
            + name_variable
            + " déclaré dans les paramètres n'existe pas dans le dictionnaire "
            + "Khiops "
            + dictionary
        )
        exit()
    return format_timestamp_khiops, format_timestamp_python, timestamp_type


def convert_format_timestamp(format_timestamp):
    if format_timestamp == "YYYY-MM-DD HH:MM:SS":
        format_timestamp = "%Y-%m-%d %H:%M:%S"

    elif format_timestamp is None:
        format_timestamp = "%Y-%m-%d %H:%M:%S"
        print(
            "Warning -> Le format du timestamp n'étant pas précisé "
            "dans le dictionnaire le format "
            "attendu est le suivant : 'YYYY-MM-DD HH:MM:SS'"
        )

    else:
        # decoupage en date - timestamp
        try:
            day, hour = format_timestamp.split()
            sep = " "
        except ValueError:
            try:
                day, hour = format_timestamp.split("T")
                sep = "T"
            except ValueError:
                print(
                    "Erreur -> Le format '"
                    + format_timestamp
                    + "' n'est pas reconnu comme un timestamp, le séparateur"
                    + " attendu entre la date et le timestamp est un espace"
                    + " ou le caractère 'T'"
                )
                exit()

        day = convert_date(day)
        hour = convert_time(hour)

        format_timestamp = sep.join([day, hour])
    return format_timestamp


def convert_format_timestampTZ(format_timestamp):
    """ ATENTION : avec le format khiops TimestampTZ 2 formats python possibles :

        # si microseconds : .147Z => .%fZ
        format_timestamp = "%Y-%m-%d %H:%M:%S.%fZ"
        ts1 = datetime.datetime(2020,1,1,12,10,2,500000)
        datetime.datetime.strftime(ts1, "%Y-%m-%d %H:%M:%S.%fZ")
            = '2020-01-01 12:10:02.500000Z'

        #/ si UTC +HH:MM ou -HH:MM => %z
        ts2 = datetime.datetime(2020, 1, 1, 12, 10, 2, tzinfo=datetime.timezone(
            datetime.timedelta(days=-1, seconds=72000)))
        datetime.datetime.strftime(ts2, "%Y-%m-%d %H:%M:%S.%z")
            = '2020-01-01 12:10:02.-0400'      # -1 jour + 20 heures = -4 heures
        format_timestamp = "%Y-%m-%d %H:%M:%S%z"

    """
    if format_timestamp == "YYYY-MM-DD HH:MM:SS.zzzzzz":
        # si microseconds : .147Z => .%fZ
        format_timestamp = "%Y-%m-%d %H:%M:%S.%fZ"
        #/ si UTC +HH:MM ou -HH:MM => %z
        format_timestamp = "%Y-%m-%d %H:%M:%S%z"

    elif format_timestamp is None:
        # si microseconds : .147Z => .%fZ
        format_timestamp = "%Y-%m-%d %H:%M:%S.%fZ"
        #/ si UTC +HH:MM ou -HH:MM => %z
        format_timestamp = "%Y-%m-%d %H:%M:%S%z"
        print(
            "Warning -> Le format du timestampTZ n'étant pas précisé "
            "dans le dictionnaire le format "
            "attendu est le suivant : 'YYYY-MM-DD HH:MM:SS.zzzzzz'"
        )

    else:
        # decoupage en date - timestamp
        try:
            day, hourz = format_timestamp.split()
            sep = " "
        except ValueError:
            try:
                day, hourz = format_timestamp.split("T")
                sep = "T"
            except ValueError:
                print(
                    "Erreur -> Le format '"
                    + format_timestamp
                    + "' n'est pas reconnu comme un timestamp, le séparateur"
                    + " attendu entre la date et le timestamp est un espace"
                    + " ou le caractère 'T'"
                )
                exit()

        day = convert_date(day)
        hour, z = removez(hourz)
        hour = convert_time(hour)

        format_timestamp = sep.join([day, hour + z])
    return format_timestamp

def removez(hourz):
    listz = ['.zzzzzz', '.zzzzz', 'zzzzzz', 'zzzzz']
    trouve = False
    for z in listz:
        hour = hourz.removesuffix(z)
        if hour != hourz:
            trouve = True
            break
    if trouve == False:
        print(
            "Erreur -> Le format TimestampTZ n'est pas reconnu. "
            + "Le suffixe doit être : '.zzzzzz', '.zzzzz', 'zzzzzz' ou 'zzzzz' "
        )
        exit()
    return hour, z

def convert_format_date(format_date):
    if format_date == "YYYY-MM-DD":
        format_date = "%Y-%m-%d"

    elif format_date is None:
        format_date = "%Y-%m-%d"
        print(
            "Warning -> Le format de la date n'étant pas précisé "
            "dans le dictionnaire le format "
            "attendu est le suivant : 'YYYY-MM-DD'"
        )

    else:
        format_date = convert_date(format_date)
    return format_date


def convert_date(day):
    """Traitement de la date"""
    car_split = ""
    # cas particuler : format sans caractère de séparation
    if len(day) == 8:
        if day[0] == "Y":
            day_split = [day[0:3], day[4:5], day[6:7]]
        else:
            day_split = [day[0:1], day[2:3], day[4:7]]

    else:
        # decoupage de la date en année - mois- jour
        for car in ["-", "/", "."]:
            day_split = day.split(car)
            if len(day_split) > 1:
                car_split = car
                break

        if car_split == "":
            print(
                "Erreur -> Le format '"
                + day
                + "' n'est pas reconnu comme une date, les séparateurs autorisés "
                + "sont : '-', '/', '.', '' "
            )
            exit()

    day_split_new = []
    for chain in day_split:
        first_car = chain[0]
        if first_car == "Y":
            day_split_new.append("%Y")
        elif first_car == "M":
            day_split_new.append("%m")
        elif first_car == "D":
            day_split_new.append("%d")
        else:
            print(
                "Erreur -> Le format '"
                + day
                + "' n'est pas reconnu comme une date, les lettres autorisées "
                + "sont : Y, M, D, par exemple YYYY-MM-DD \n"
                + "Se référer à la doc Khiops pour la liste des formats autorisés"
            )
            exit()

    day = car_split.join(day_split_new)
    #print("format_date >> " + day)
    return day


def convert_time(hour):
    """Traitement de la partie timestamp"""
    car_split_h = ""
    # suppression du point à la fin si présent
    if hour[-1] == ".":
        hour = hour[:-1]

    # cas particuler : format sans caractère de séparation
    if len(hour) == 4:
        hour_split = [hour[0:1], hour[2:3]]

    elif len(hour) == 6:
        hour_split = [hour[0:1], hour[2:3], hour[4:5]]

    else:
        # decoupage du timestamp en heure - min - sec

        for car in [":", "."]:
            hour_split = hour.split(car)

            if len(hour_split) > 1:
                car_split_h = car
                break

        if car_split_h == "":
            print(
                "Erreur -> Le format '"
                + hour
                + "' n'est pas reconnu comme un format heure, les séparateurs "
                + "autorisés sont : ':', '.', '' "
            )
            exit()

    hour_split_new = []
    for chain in hour_split:
        first_car = chain[0]
        if first_car == "(":
            first_car = chain[1]
        if first_car == "H":
            hour_split_new.append("%H")
        elif first_car == "M":
            hour_split_new.append("%M")
        elif first_car == "S":
            hour_split_new.append("%S")
        else:
            print(
                "Erreur -> Le format '"
                + hour
                + "' n'est pas reconnu comme un format heure, "
                + "les lettres autorisées sont :"
                + " H, M, S, par exemple HH:MM:SS"
            )
            exit()

    hour = car_split_h.join(hour_split_new)
    return hour


def supp_car_datetime(datetime_str):
    """
    Pour le nom des tables dans Khiops,
    transformation "YYYY-MM-DD HH:MM:SS" en "YYYYMMDDHHMMSS" 
    """
    datetime_str = datetime_str.replace(" ", "")
    datetime_str = datetime_str.replace("-", "")
    datetime_str = datetime_str.replace(":", "")
    return datetime_str

def convert_to_seconds(timestamp_type, period_unit):
    """
    pour les calculs d'intervalle dans le dico khiops
    """
    if timestamp_type == "Date":
        nb_seconds = "1"
    elif timestamp_type == "Timestamp":
        if period_unit == "days":
            nb_seconds = "86400"
        elif period_unit == "hours":
            nb_seconds = "3600"
        elif period_unit == "minutes":
            nb_seconds = "60"
    return nb_seconds
  
def double_transform(mycolumn, format_timestamp):
    mycolumn = mycolumn.apply(
        lambda x: datetime.datetime.strftime(
            datetime.datetime.strptime(
                x,
                format_timestamp,
            ),
            format_timestamp,
        )
    )
    return mycolumn

def exist_datamart(data_tables):
    is_datamart = True
    try:
        data_tables["entities"]
        if data_tables["entities"] == {}:
            is_datamart = False
    except KeyError:
        is_datamart = False

    return is_datamart

def exist_table_with_datetime(data_tables):
    # déclaration d'au moins une table
    is_table_w_datetime = True
    try:
        data_tables["tables"]
        if data_tables["tables"] == {}:
            is_table_w_datetime = False
    except KeyError:
        is_table_w_datetime = False
    
    if not is_table_w_datetime:
        print("Erreur -> Il est nécessaire de déclarer au moins une table "
              + 'dans data_tables["tables"]')
        exit()
    
    # déclaration d'au moins un datetime
    is_datetime = False
    for table in data_tables["tables"]:
        try:
            data_tables["tables"][table]["datetime"]
            is_datetime = True
            break
        except :
            pass

    if not is_datetime:
        print("Erreur -> Il est nécessaire de déclarer au moins un datetime "
              + "pour l'une des tables dans "
              + 'data_tables["tables"][my_table]["datetime"]')
        exit()            

def exist(file):
    try:
        with open(file):
            pass
    except IOError:
        print("Erreur -> Le fichier " + file + " n'a pas pu être ouvert")
        exit()

def exist_in_dico_khiops(dico_domain, var_name):
    for dico in dico_domain.dictionaries:
        if dico.root:
            break

    trouve = False
    for var in dico.variables:
        if var.name == var_name:
            trouve = True
            break
    if not trouve:
        print(
            "La variable '"
            + var_name + "' déclarée dans data_tables"
            + " n'existe pas dans le dictionnaire Khiops"
        )
        exit()

def parse_name_file(name_file):
    """Extraction du nom du fichier et de son extension"""
    root, extension = path.splitext(name_file)
    return root, extension


def creation_list_datamarts_datetime(data_tables):
    if exist_datamart(data_tables):
        for key in data_tables["entities"].keys():
            # creation de la liste des datetime disponibles dans la définition
            # des datamarts
            list_datamarts_datetime = []
            len_datamart = len(data_tables["entities"][key])
            for i in range(len_datamart):
                datamart_datetime = data_tables["entities"][key][i]["datetime"]
                if datamart_datetime not in list_datamarts_datetime:
                    list_datamarts_datetime.append(datamart_datetime)

            list_datamarts_datetime = sorted(list_datamarts_datetime)

            if list_datamarts_datetime == []:
                print(
                    "pas de datetime trouvé, veuillez renseigner "
                    + "les datetime des tables entities "
                )
                exit()
    return list_datamarts_datetime


def create_map_tables(data_tables):
    map_tables = {}

    for key in data_tables["tables"].keys():
        file_log = data_tables["tables"][key]["file_name"]
        exist(file_log)
        map_tables[key] = file_log

    return map_tables


def create_list_entities(data_tables):
    """
    ['DataMart1', 'DataMart2', ...]
    """
    list_entities = []
    
    if exist_datamart(data_tables):
        for key in data_tables["entities"].keys():
            list_entities.append(key)

    return list_entities

# create_map_entities() ne sert plus
def create_map_entities(data_tables):
    """
    {('DataMart1', '2019-09-01'): '.../sample2_synthetic_DM1_07.csv',
     ('DataMart1', '2019-10-01'): '.../sample2_synthetic_DM1_08.csv',
     ('DataMart1', '2019-11-01'): '.../sample2_synthetic_DM1_09.csv'}
    """
    map_entities = {}

    if exist_datamart(data_tables):
        for key in data_tables["entities"].keys():
            len_datamart = len(data_tables["entities"][key])
            for i in range(len_datamart):
                datamart_datetime = data_tables["entities"][key][i]["datetime"]
                datetime_str = str(datamart_datetime)

                file_entity = data_tables["entities"][key][i]["file_name"]
                exist(file_entity)
                map_entities[key, datetime_str] = file_entity

    return map_entities

# create_map_entities_number() ne sert plus
def create_map_entities_number(data_tables):
    """
    {'DataMart10': '.../sample2_synthetic_DM1_07.csv',
     'DataMart11': '.../sample2_synthetic_DM1_08.csv',
     'DataMart12': '.../sample2_synthetic_DM1_09.csv'}
    """
    # remarque: dans khiops on ne peut pas donner à la table le nom
    # "key + datetime" (à cause des tirets)
    # error: Variable `name_of_the_first_entity_table_2019-09-01` :
    #    Incorrect name for a native variable of type Entity:
    #       must not contain back-quote
    # solution : on numérote 
    map_entities_number = {}   
    # crée le dictionnaire de toutes les
    # entities {key, datetime, key_with_number}
    # pour tous les datetime

    for key in data_tables["entities"].keys():

        len_datamart = len(data_tables["entities"][key])
        for i in range(len_datamart):

            file_entity = data_tables["entities"][key][i]["file_name"]
            map_entities_number[key] = file_entity

    return map_entities_number

def create_map_entities_datetime(data_tables):
    """
    {('DataMart1', '2019-09-01', 'DataMart10'): 
             '.\\notebook\\data\\synth1\\sample2_synthetic_DM1_07.csv', 
     ('DataMart1', '2019-10-01', 'DataMart11'): 
             '.\\notebook\\data\\synth1\\sample2_synthetic_DM1_08.csv', 
     ('DataMart1', '2019-11-01', 'DataMart12'): 
             '.\\notebook\\data\\synth1\\sample2_synthetic_DM1_09.csv'}
    """
    # remarque: dans khiops on ne peut pas donner à la table le nom
    # "key + datetime" (à cause des tirets)
    # error: Variable `name_of_the_first_entity_table_2019-09-01` :
    #    Incorrect name for a native variable of type Entity:
    #       must not contain back-quote
    # solution : on numérote 
    map_entities_datetime = {}   
    # crée le dictionnaire de toutes les
    # entities {key, datetime, key_with_number}
    # pour tous les datetime

    for key in data_tables["entities"].keys():

        len_datamart = len(data_tables["entities"][key])
        for i in range(len_datamart):

            file_entity = data_tables["entities"][key][i]["file_name"]
            map_entities_datetime[
                key,
                str(data_tables["entities"][key][i]["datetime"]),                    
                key + str(i)
            ] = file_entity

    #for (key, datetime_str, key_number) in map_entities_datetime:
    #    print(map_entities_datetime[key, datetime_str, key_number])

    return map_entities_datetime

def compare_data_tables_with_dictionary(
        dictionary, 
        data_tables, 
        target_parameters
    ):
    """
    Vérification de la cohérence entre
        - les tables dans "data_tables"
        - le dictionnaire Khiops "dictionary"

    Correspondance entre les noms des tables et les noms des variables déclarées
    """
    # dictionnaire Khiops
    dico_domain = kh.read_dictionary_file(dictionary)

    # table principale
    # nom de la table
    try:
        name_main = data_tables["main_table"]["name_main_table"]
    except KeyError:
        print(
            "La table principale doit être déclarée dans le dictionnaire : "
            + '\ndata_tables["main_table"]["name_main_table"] '
        )
        exit()
        
    for dico in dico_domain.dictionaries:
        if dico.root:
            name_root = dico.name
            break
    try:
        name_root
    except NameError:
        print(
            "La table principale n'est pas identifiée dans le dictionnaire Khiops."
            + "\nVeuillez l'indiquer par 'Root Dictionary' "
        )
        exit()
    if name_main != name_root:
        print(
            "le nom de la table principale déclaré dans data_tables '"
            + name_main
            + "' ne correspond pas au nom dans le dictionnaire Khiops '"
            + name_root
            + "'"
        )
        exit()
    # les variables
    # id
    try:
        ident = data_tables["main_table"]["key"]
    except KeyError:
        print(
            "L'identifiant de la table principale doit être renseigné dans "
             + 'data_tables["main_table"]["key"]'
        )
        exit()
    trouve = False
    for var in dico.variables:
        if var.name == ident:
            trouve = True
            break
    if not trouve:
        print(
            "L'identifiant '"
            + ident + "' déclaré dans data_tables"
            + " n'existe pas dans le dictionnaire Khiops"
        )
        exit()
    # target
    try:
        target_parameters["target"]
    except KeyError:
        print(
            "La cible de la table principale doit être renseignée "
             + 'dans target_parameters["target"]'
        )
        exit()
    """ déplacé dans fit pour vérifier que la variable existe dans le dico khiops
    trouve = False
    for var in dico.variables:
        if var.name == target:
            trouve = True
            break
    if not trouve:
        print(
            "La target '"
            + target + "' déclarée dans data_tables"
            + " n'existe pas dans le dictionnaire Khiops"
        )
        exit()
    """
    # datetime
    try:
        target_parameters["datetime"]
    except KeyError:
        print(
            "La variable temporelle de la table principale doit être renseignée "
             + 'dans target_parameters["datetime"]'
        )
        exit()
    """ déplacé dans fit pour vérifier que la variable existe dans le dico khiops
    trouve = False
    for var in dico.variables:
        if var.name == datetime_var:
            trouve = True
            break
    if not trouve:
        print(
            "La variable datetime '"
            + datetime_var + "' déclarée dans data_tables"
            + " n'existe pas dans le dictionnaire Khiops"
        )
        exit()
    """
    # existence du fichier
    exist(data_tables["main_table"]["file_name"])

    # tables secondaires
    exist_table_with_datetime(data_tables)
    
    for table in data_tables["tables"]:
        # nom de la table
        table_name = table
        trouve = False
        for dico in dico_domain.dictionaries:
            if dico.name == table_name:
                trouve = True
                break
        if not trouve:
            print(
                "La table '"
                + table + "' déclarée dans data_tables"
                + " n'existe pas dans le dictionnaire Khiops"
            )
            exit()
        # les variables
        # id
        try:
            ident = data_tables["tables"][table]["key"]
        except KeyError:
            print(
                "L'identifiant de la table '"
                + table + "' doit être renseigné dans "
                 + 'data_tables["tables"]["' + table + '"]["key"]'
            )       
            exit()
        trouve = False
        for var in dico.variables:
            if var.name == ident:
                trouve = True
                break
        if not trouve:
            print(
                "L'identifiant '"
                + ident + "' déclaré dans data_tables"
                + " n'existe pas dans le dictionnaire Khiops"
            )
            exit()
        # datetime (facultatif)
        try:
            datetime_var = data_tables["tables"][table]["datetime"]
        # la variable n'existe pas
        except KeyError:
            print(
                "Si la table '" + table 
                + "' comporte une variable temporelle "
                + "elle doit être renseignée dans "
                + 'data_tables["tables"]["' + table + '"]["datetime"], '
                + "sinon cette table ne sera pas être prise en compte "
                + "pour la sélection des logs")
        # si elle existe vérifier qu'elle est dans le dicio Khiops
        else :          
            trouve = False
            for var in dico.variables:
                if var.name == datetime_var:
                    trouve = True
                    break
            if not trouve:
                print(
                    "La variable datetime '"
                    + datetime_var + "' déclarée dans data_tables : "
                    + 'data_tables["tables"][' + table + ']["datetime"]'
                    + " n'existe pas dans le dictionnaire Khiops"
                )
                exit()
        # existence du fichier
        exist(data_tables["tables"][table]["file_name"])

    # datamarts
    if exist_datamart(data_tables):
        for entity in data_tables["entities"].keys():
            # boucle sur les datamarts
            len_datamart = len(data_tables["entities"][entity])

            # nom de la table
            entity_name = entity
            trouve = False
            for dico in dico_domain.dictionaries:
                if dico.name == entity_name:
                    trouve = True
                    break
            if not trouve:
                print(
                    "La table '"
                    + entity + "' déclarée dans data_tables"
                    + " n'existe pas dans le dictionnaire Khiops"
                )
                exit()
            for i in range(len_datamart):
                # les variables
                # id
                try:
                    ident = data_tables["entities"][entity][i]["key"]
                except KeyError:
                    print(
                        "L'identifiant de la table '"
                        + entity + "' doit être renseigné dans "
                        + 'data_tables["entities"]["' + entity
                        + '"][' + str(i) + ']["key"]'
                    )  
                    exit()
                trouve = False
                for var in dico.variables:
                    if var.name == ident:
                        trouve = True
                        break
                if not trouve:
                    print(
                        "L'identifiant '"
                        + ident + "' déclarée dans data_tables"
                        + " n'existe pas dans le dictionnaire Khiops"
                    )
                    exit()
                # existence du fichier
                try:
                    fichier = data_tables["entities"][entity][i]["file_name"]
                except KeyError:
                    print(
                        "Le fichier de données doit être renseigné dans "
                        + 'data_tables["entities"]["' + entity
                        + '"][' + str(i) + ']["file_name"]'
                    )  
                    exit()                    
                exist(fichier)

def compare_dictionary_with_data_tables(
        dictionary, 
        data_tables, 
        target_parameters,
    ):
    """
    Vérification de la cohérence entre
        - le dictionnaire Khiops "dictionary"
        - les tables dans "data_tables"
    """
    # dictionnaire des tables
    map_tables = create_map_tables(data_tables)
    
    # liste des entities
    list_entities = create_list_entities(data_tables)
    
    # dictionnaire Khiops
    dico_domain = kh.read_dictionary_file(dictionary)

    for dico in dico_domain.dictionaries:
        if not dico.root:
            name_table_logs = dico.name
            # recherche de la table dans les noms de tables déclarées
            for key in map_tables.keys():
                key_flag = False
                # pour chaque nom de table dans Khiops on cherche si la table
                # est bien déclarée dans les tables de data_tables 
                if key == name_table_logs:
                    key_flag = True
                    my_timestamp_flag = False
                    try:
                        my_timestamp = data_tables["tables"][key]["datetime"]
                    except KeyError:
                        break
                    for var in dico.variables:
                        if var.type == "Timestamp":
                            if var.name == my_timestamp:
                                my_timestamp_flag = True
                                break

                    if not my_timestamp_flag:
                        print(
                            "La variable de type Timestamp '"
                            + my_timestamp
                            + "' est manquante dans le dictionnaire "
                            + name_table_logs
                        )
                        exit()
                    break
            
            if not key_flag:
                # recherche de la table dans les noms d'entities déclarées
                for key in list_entities:
                    # pour chaque nom de table dans Khiops on cherche si la
                    # table est bien déclarée dans les entities de data_tables
                    if key == name_table_logs:
                        key_flag = True
                        break

            if not key_flag:
                print(
                    "Le nom de la table '"
                    + name_table_logs
                    + "' dans le dictionnaire Khiops '"
                    + dictionary
                    + "' ne correspond à aucune des tables "
                    "déclarées dans data_tables"
                )
                exit()

def work_path(rep, mobile, path_suffix):
    if mobile:
        rep_results = "mobile" + path_suffix
    else:
        rep_results = "fixe" + path_suffix
    return path.join(rep, rep_results)