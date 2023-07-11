######################################################################################
# Copyright (c) 2023 Orange - All Rights Reserved                             #
# * This software is the confidential and proprietary information of Orange.         #
# * You shall not disclose such Restricted Information and shall use it only in      #
#   accordance with the terms of the license agreement you entered into with Orange  #
#   named the "Kastor - Python Library Evaluation License".                          #
# * Unauthorized copying of this file, via any medium is strictly prohibited.        #
# * See the "LICENSE.md" file for more details.                                      #
######################################################################################
from datetime import datetime
from os import path
from pykhiops import core as pk
from sys import exit


def detect_format_timestamp(dictionary, name_variable):
    """Détection dans le dictionnaire khiops du format renseigné
    pour un Timestamp ou une Date"""
    dico_domain = pk.read_dictionary_file(dictionary)
    for dico in dico_domain.dictionaries:
        for var in dico.variables:
            if var.name == name_variable:
                if var.type == "Date":
                    try:
                        format_timestamp = var.meta_data.get_value(
                            "DateFormat"
                        )
                        format_timestamp = convert_format_date(
                            format_timestamp
                        )
                        break
                    except KeyError:
                        format_timestamp = "%Y-%m-%d"

                elif var.type == "Timestamp":
                    try:
                        format_timestamp = var.meta_data.get_value(
                            "TimestampFormat"
                        )
                        format_timestamp = convert_format_timestamp(
                            format_timestamp
                        )
                        break
                    except KeyError:
                        format_timestamp = "%Y-%m-%d %H:%M:%S"
                else:
                    print(
                        "Erreur -> La variable "
                        + name_variable
                        + " doit être de type Date ou Timestamp"
                    )
                    exit()
    return format_timestamp


def convert_format_timestamp(format_timestamp):
    if format_timestamp == "YYYY-MM-DD HH:MM:SS":
        format_timestamp = "%Y-%m-%d %H:%M:%S"

    elif format_timestamp is None:
        format_timestamp = "%Y-%m-%d %H:%M:%S"
        print(
            "Warning -> Le format du timestamp n'étant pas précisé dans le dictionnaire le format "
            "attendu est le suivant : 'YYYY-MM-DD HH:MM:SS'"
        )

    else:
        # decoupage en date - timestamp
        try:
            day, hour = format_timestamp.split()
        except ValueError:
            print(
                "Erreur -> Le format "
                + format_timestamp
                + "n'est pas reconnu comme un timestamp, le séparateur attendu "
                " entre la date et le timestamp est un espace"
            )
            exit()

        day = convert_date(day)
        hour = convert_time(hour)

        format_timestamp = " ".join([day, hour])
    return format_timestamp


def convert_format_date(format_date):
    if format_date == "YYYY-MM-DD":
        format_date = "%Y-%m-%d"

    elif format_date is None:
        format_date = "%Y-%m-%d"
        print(
            "Warning -> Le format de la date n'étant pas précisé dans le dictionnaire le format "
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
                "Erreur -> Le format "
                + day
                + " n'est pas reconnu comme une date, les séparateurs autorisées sont :"
                " '-', '/', '.', '' "
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
                "Erreur -> Le format "
                + day
                + " n'est pas reconnu comme une date, les lettres autorisées sont :"
                " Y, M, D, par exemple YYYY-MM-DD"
            )
            exit()

    day = car_split.join(day_split_new)
    print("format_date >> " + day)
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
                "Erreur -> Le format "
                + hour
                + " n'est pas reconnu comme un format heure, les séparateurs autorisées sont :"
                " ':', '.', '' "
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
                "Erreur -> Le format "
                + hour
                + " n'est pas reconnu comme un format heure, les lettres autorisées sont :"
                " H, M, S, par exemple HH:MM:SS"
            )
            exit()

    hour = car_split_h.join(hour_split_new)
    return hour


def supp_car_datetime(datetime_str):
    """Pour le nom des tables dans Khiops,
    transformation "YYYY-MM-DD HH:MM:SS" en "YYYYMMDDHHMMSS" """
    datetime_str = datetime_str.replace(" ", "")
    datetime_str = datetime_str.replace("-", "")
    datetime_str = datetime_str.replace(":", "")
    return datetime_str


def exist_datamart(data_tables):
    is_datamart = True
    try:
        data_tables["entities"]
    except KeyError:
        is_datamart = False

    return is_datamart


def exist(file):
    try:
        with open(file):
            pass
    except IOError:
        print("Erreur -> Le fichier " + file + " n'a pas pu être ouvert")
        exit()


def parse_name_file(name_file):
    """Extraction du nom du fichier et de son extension"""
    root, extension = path.splitext(name_file)
    return root, extension


def creation_list_datamarts_datetime(data_tables, format_timestamp_target):
    for key in data_tables["entities"].keys():
        # creation de la liste des datetime disponibles dans la définition
        # des datamarts
        list_datamarts_datetime = []
        len_datamart = len(data_tables["entities"][key])
        for i in range(len_datamart):
            datamart_datetime = data_tables["entities"][key][i]["datetime"]
            if datamart_datetime not in list_datamarts_datetime:
                list_datamarts_datetime.append(datamart_datetime)

        try:
            list_datamarts_datetime = sorted(list_datamarts_datetime)
        except ValueError:
            print(
                "les datetime des tables entities doivent être exprimés dans le même format que la target : "
                + format_timestamp_target
            )
            exit()
    return list_datamarts_datetime


def create_map_tables_entities(data_tables, datetime_str=""):
    map_tables = create_map_tables(data_tables)
    map_entities = create_map_entities(data_tables, datetime_str)

    map_tables_entities = map_tables.copy()
    map_tables_entities.update(map_entities)

    return map_tables_entities


def create_map_tables(data_tables):
    map_tables = {}

    for key in data_tables["tables"].keys():
        file_log = data_tables["tables"][key]["file_name"]
        exist(file_log)
        map_tables[key] = file_log

    return map_tables


def create_map_entities(data_tables, datetime_str=""):
    """Pour une période fixe, crée le dictionnaire des entities
    {key, name_file} pour un datetime"""
    map_entities = {}

    if exist_datamart(data_tables):
        if datetime_str != "":
            for key in data_tables["entities"].keys():
                len_datamart = len(data_tables["entities"][key])
                find = False
                for i in range(len_datamart):
                    datamart_datetime = data_tables["entities"][key][i][
                        "datetime"
                    ]
                    if datamart_datetime == datetime_str:
                        find = True
                        file_entity = data_tables["entities"][key][i][
                            "file_name"
                        ]
                        exist(file_entity)
                        map_entities[key] = file_entity
                        break
                if not find:
                    print(
                        "le datamart '"
                        + key
                        + "' doit comporter le datetime suivant : "
                        + datetime_str
                    )
                    exit()

    return map_entities


def work_path(rep, mobile):
    if mobile:
        rep_results = "mobile"
    else:
        rep_results = "fixe"
    return path.join(rep, rep_results)
