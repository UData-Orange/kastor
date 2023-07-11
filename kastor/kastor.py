######################################################################################
# Copyright (c) 2023 Orange - All Rights Reserved                             #
# * This software is the confidential and proprietary information of Orange.         #
# * You shall not disclose such Restricted Information and shall use it only in      #
#   accordance with the terms of the license agreement you entered into with Orange  #
#   named the "Kastor - Python Library Evaluation License".                          #
# * Unauthorized copying of this file, via any medium is strictly prohibited.        #
# * See the "LICENSE.md" file for more details.                                      #
######################################################################################
"""
Module de génération des datasets train et test

Librairie de scoring : fit, predict, evaluate, plot.
"""
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random

from datetime import datetime, timedelta
from os import path
from pykhiops import core as pk
from sys import exit
from time import process_time

from kastor._timeevalscore import ProactiveEvalScore, ReactiveEvalScore
from kastor._util import (
    creation_list_datamarts_datetime,
    create_map_entities,
    create_map_tables,
    detect_format_timestamp,
    exist,
    exist_datamart,
    parse_name_file,
    work_path,
)


class Dataset:
    """Classe pour générer les datasets train et test correspondant aux
    intervalles définis par l'utilisateur.

    Parameters
    ----------
    dictionary
        Dictionnaire Khiops décrivant les données

    data_table
        Définition des données et de leurs liens key et datetime

        .. note:: Contenu de **data_tables**

            "main_table": Nom de la table principale avec la cible horodatée

                | name_main_table : name of the main table (same as khiops)
                | file_name : file name with path
                | key : name of the id variable

            "entities": Datamarts

                "name_of_the_first_entity_table" : NV c'est une liste de
                fichiers datamarts caractérisés par leur datetime

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": "2019-09-01"

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": "2019-10-01"

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": "2019-11-01"

                    .. warning:: "datetime", dernier jour non
                        compris, est à exprimer dans la même unité que
                        *format_timestamp_target*.

                "name_of_the_second_entity_table"

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": "2019-09-01"

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": "2019-10-01"

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": "2019-11-01"

            "tables" : tables de logs

                "sample2_logs_churn_xdsl" :

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": name of the datetime variable

                "name of the second table" :

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": name of the datetime variable

    target_parameters
        Paramètres liés à la cible (dans la table principale)

        .. note:: Contenu de **target_parameters**

            | "datetime": name of the datetime variable,
            | "target": name of the target variable,
            | "main_target_modality": main target modality, optional
            | "default_target_modality": default target name, optional

    temporal_parameters
        Paramètres temporels de l'étude

        .. note:: Contenu de **temporal_parameters**

            | "period_unit" : time unit for fit and for predict,
            | "input_data_duration" : # L # durée de prise en compte des logs
            | "model_gap" : model gap,
            | "target_duration" : # l # profondeur d'observation de la cible
            | "start_date": target start date,
            | "target_end_date": target end date, optional
            | "period_nb": durée de la période de scores

            .. warning:: "input_data_duration", "target_duration" et
                "period_nb" sont à exprimer dans la même unité que *period_unit*.

    sep : str, default ' \\ t ' ?
        Séparateur des fichiers de données qui est le même séparateur pour
        toutes les tables

    mobile : bool, default True
        Explication ?
    """

    def __init__(
        self,
        dictionary,
        data_tables,
        target_parameters,
        temporal_parameters,
        sep="\t",
        mobile=True,
    ):
        self.dictionary = dictionary
        self.data_tables = data_tables
        self.target_parameters = target_parameters
        self.temporal_parameters = temporal_parameters
        self.sep = sep
        self.mobile = mobile

    def _generate_timestamp_target(self):
        """Génération des timestamps aléatoires pour la cible 0

            Sélection des cibles = 1 dont les dates sont comprises dans
            l'intervalle spécifié
                Cible = 1 et date ok

            Pour les autres on affecte cible = 0 et tirage d'une date :
                Soit dans les logs si l'intervalle de dates recouvre celui des
                dates de cible, sinon dans les dates de cible

        Return
        -------
        df_target : dataframe
            Le dataframe comportant les cibles correspondant aux intervalles
            spécifiés et les dates générées aléatoirement quand nécessaire

        """

        # detection de format_timestamp_target
        name_var_date_target = self.target_parameters["datetime"]
        format_timestamp_target = detect_format_timestamp(
            self.dictionary, name_var_date_target
        )

        # chargement du fichier cible
        file_target = self.data_tables["main_table"]["file_name"]
        exist(file_target)

        df_target = pd.read_csv(
            file_target, sep=self.sep, encoding="ISO-8859-1"
        )
        print(file_target + " --> " + str(len(df_target)) + " lignes\n")

        print(df_target.groupby([self.target_parameters["target"]]).count())
        print("\n")

        date_target_not_null = df_target[
            ~df_target[self.target_parameters["datetime"]].isnull()
        ]
        date_target_not_null = date_target_not_null[
            self.target_parameters["datetime"]
        ]
        ts_date_target_not_null = pd.to_datetime(
            date_target_not_null, format=format_timestamp_target
        )

        ts_date_target_min = min(ts_date_target_not_null)
        ts_date_target_max = max(ts_date_target_not_null)
        print("timestamp min: " + str(ts_date_target_min))
        print("timestamp max: " + str(ts_date_target_max))

        # verification des intervalles de dates
        start_date_target = pd.Timestamp(
            self.temporal_parameters["start_date"]
        )
        end_date_target = pd.Timestamp(
            self.temporal_parameters["target_end_date"]
        )
        if start_date_target < ts_date_target_min:
            raise ValueError(
                "la date de debut de cible specifiee "
                + str(start_date_target)
                + " est inferieure a la date minimale des donnees "
                + str(ts_date_target_min)
            )
        if end_date_target > ts_date_target_max:
            raise ValueError(
                "la date de fin de cible specifiee "
                + str(end_date_target)
                + " est superieure a la date maximale des donnees "
                + str(ts_date_target_max)
            )

        # chargement des logs
        self.tirage = "logs"
        for key in self.data_tables["tables"].keys():
            file_log = self.data_tables["tables"][key]["file_name"]
            exist(file_log)
            df_logs = pd.read_csv(
                file_log, sep=self.sep, encoding="ISO-8859-1"
            )

            # detection de format_timestamp_log
            name_var_timestamp_log = self.data_tables["tables"][key][
                "datetime"
            ]
            format_timestamp_log = detect_format_timestamp(
                self.dictionary, name_var_timestamp_log
            )
            format_timestamp_log = "%d/%m/%Y %H:%M:%S"
            print(
                "\n\n" + file_log + " --> " + str(len(df_logs)) + " lignes\n"
            )
            print("timestamp log format : " + format_timestamp_log + "\n")
            ts_date_log = pd.to_datetime(
                df_logs[self.data_tables["tables"][key]["datetime"]],
                format=format_timestamp_log,
            )

            ts_date_log_min = min(ts_date_log)
            ts_date_log_max = max(ts_date_log)
            print("timestamp min: " + str(ts_date_log_min))
            print("timestamp max: " + str(ts_date_log_max))

            # verification des intervalles de dates pour tirage des dates pour
            # les cibles=0 dans les logs
            # si la periode des logs recouvre celle des cibles tirage dans les
            # logs, sinon tirage dans les cibles
            if (start_date_target < ts_date_log_min) or (
                end_date_target > ts_date_log_max
            ):
                self.tirage = "cible"

        # constitution du fichier cible

        ###################### A TRAITER : SI ON NE PREND PAS TOUTES LES CIBLES

        # on garde toutes les cibles = 1 comprises entre start_date et end_date
        df_target_1 = df_target[
            df_target[self.target_parameters["target"]]
            == self.target_parameters["main_target_modality"]
        ]
        df_target_1 = df_target_1[
            (
                pd.to_datetime(
                    df_target_1[self.target_parameters["datetime"]],
                    format=format_timestamp_target,
                )
                >= start_date_target
            )
            & (
                pd.to_datetime(
                    df_target_1[self.target_parameters["datetime"]],
                    format=format_timestamp_target,
                )
                <= end_date_target
            )
        ]

        nb_target_1 = len(df_target_1)
        print(
            "\nNombre de cibles avec modalite "
            + str(self.target_parameters["main_target_modality"])
            + " comprises dans l intervalle specifie : "
            + str(nb_target_1)
        )

        # on complète avec les cibles = 0 et cible = 1 hors bornes
        df_target_0 = df_target.drop(df_target_1.index)
        nb_target_0 = len(df_target_0)
        df_target_0[self.target_parameters["target"]] = self.target_parameters[
            "default_target_modality"
        ]
        df_target_0[self.target_parameters["datetime"]] = np.nan

        # tirage aléatoire d'une date de cible pour les cibles = 0

        if self.tirage == "logs":
            # tirage parmi les logs, soit creer le fichier de logs sur la même
            # période que la cible
            # on prend ici le fichier de logs en memoire,
            # soit le dernier de la liste

            df_date_logs = df_logs[self.data_tables["tables"][key]["datetime"]]
            df_date_for_target = df_date_logs.loc[
                (
                    pd.to_datetime(df_date_logs, format=format_timestamp_log)
                    >= start_date_target
                )
                & (
                    pd.to_datetime(df_date_logs, format=format_timestamp_log)
                    <= end_date_target
                )
            ]

        else:
            # tirage parmi les cibles
            df_date_for_target = df_target_1[
                self.target_parameters["datetime"]
            ]

        list_index = df_date_for_target.index

        # boucle sur toutes les lignes pour affecter une date
        random.seed(666)
        t = process_time()
        if self.tirage == "logs":
            for n in range(nb_target_0):
                index = random.choice(list_index)
                ts_str = df_date_for_target[index]
                ts = datetime.strptime(ts_str, format_timestamp_log).strftime(
                    format_timestamp_target
                )
                df_target_0.iloc[n, 1] = ts

        else:
            for n in range(nb_target_0):
                index = random.choice(list_index)
                ts_str = df_date_for_target[index]
                df_target_0.iloc[n, 1] = ts_str

        print(
            "\nDuree d execution de l'affectation d une date aleatoire aux cibles "
            + str(self.target_parameters["default_target_modality"])
            + " : "
            + str(round(process_time() - t))
            + "s"
        )
        # --> 388.4241736256332 = 6,5 minutes

        # concatenation des cibles = 0 et cibles = 1
        df_target = pd.concat([df_target_1, df_target_0], ignore_index=True)
        df_target = df_target.sort_values(
            by=self.data_tables["main_table"]["key"]
        )
        df_target = df_target.reset_index(drop=True)
        return df_target

    def generate_train_test(
        self,
        percentage_train=0.7,
        effectif_target=0,
        effectif_no_target=0,
    ):
        """Génération des datasets de train et test

        Parameters
        ----------
        percentage_train : default 0.7
            Proportion des données pour le dataset de train
        effectif_target :  default 0
            Effectif à attribuer au dataset de train, à utiliser avec
            effectif_no_target, si specifié percentage_train est ignoré
        effectif_no_target : default 0
            Effectif à attribuer au dataset de test, à utiliser avec
            effectif_target, si specifié percentage_train est ignoré

        Returns
        -------
        df_train : fichier csv
        df_test : fichier csv
            Les fichiers train et test sont écrits dans le répertoire des
            données
        """

        df_target = self._generate_timestamp_target()

        # effectif_target = kwargs.get("effectif_target", 0)
        # effectif_no_target = kwargs.get("effectif_no_target", 0)

        if (effectif_target > 0) & (effectif_no_target > 0):
            nb_train = effectif_target
            nb_test = effectif_no_target
            df_test = df_target.sample(
                n=nb_test, random_state=666, replace=False
            ).sort_index(axis=0)
            df_remaining = df_target.drop(df_test.index)
            df_train = df_remaining.sample(
                n=nb_train, random_state=1906, replace=False
            ).sort_index(axis=0)

        else:
            # percentage_train = kwargs.get("percentage_train", 0.7)
            nb_test = round(len(df_target) * (1 - percentage_train))
            df_test = df_target.sample(
                n=nb_test, random_state=666, replace=False
            ).sort_index(axis=0)
            df_remaining = df_target.drop(df_test.index)
            df_train = df_remaining

        file_target = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_target)

        # écriture des fichiers train et test
        name_df_train = path.join(rep, "train_" + file)
        df_train.to_csv(name_df_train, sep=self.sep, index=False)
        name_df_test = path.join(rep, "test_" + file)
        df_test.to_csv(name_df_test, sep=self.sep, index=False)

    def _modif_selection_dico_khiops_for_fit(
        self,
        map_entities_train,
        name_var_date_target,
        start_date,
        period_unit,
        model_gap,
        input_data_duration,
    ):
        """
        Modification du dictionnaire à la volée pour sélection des logs

            Période fixe

                Table principale :

                    Unused	Table(logs)	logs;

                    Table(logs)	logsSelection =
                        TableSelection(logs, And(
                            GE(Sum(DiffDate( GetDate(my_timestamp),
                            AsDate("2019-07-04","YYYY-MM-DD")), 7) ,0),
                            LE(Sum(DiffDate( GetDate(my_timestamp),
                            AsDate("2019-09-02","YYYY-MM-DD")), 7) ,0)));

            Période mobile :

                Table secondaire :

                    Unused Entity(sampledatamart) principal [line_id_sha];

                    Unused Numerical delta_jours =
                        DiffDate(GetValueD(principal, date_souscription),
                        GetDate(my_timestamp));

                    Unused Numerical delta_target =
                        GetValue(principal, delta_target_random);

                Table principale :

                    Unused Table(logs) logs;

                    Table(logs)	logsSelection =
                        TableSelection(logs, And(
                            LE(delta_jours, Sum(1, delta_target, 90)),
                            GE(delta_jours, Sum(1, delta_target))));

        """

        dico_domain = pk.read_dictionary_file(self.dictionary)

        # Dictionnaires logs : recuperation du nom des tables et du nom des
        # variables Timestamp dans le dictionnaire Khiops
        map_tables_timestamp = {}
        additional_table = {}

        # recuperation des path des tables dans data_tables
        # ce ne sera pas le même ordre que le dico khiops
        map_tables = create_map_tables(self.data_tables)

        # verication de l'existence des entities
        for key, file in map_entities_train.items():
            exist(file)

        for dico in dico_domain.dictionaries:
            if dico.root:
                name_root = dico.name
                break

        for dico in dico_domain.dictionaries:
            if not dico.root:
                name_table_logs = dico.name
                # recherche de la table dans les noms de tables déclarées
                for key in map_tables.keys():
                    key_flag = False
                    # pour chaque nom de table dans Khiops on cherche la table
                    # qui correspond dans data_tables pour récupérer le path
                    if key == name_table_logs:
                        key_flag = True
                        additional_table[
                            name_root + "`" + name_table_logs
                        ] = map_tables[key]
                        my_timestamp_flag = False
                        my_timestamp = self.data_tables["tables"][key][
                            "datetime"
                        ]
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
                        map_tables_timestamp[name_table_logs] = my_timestamp
                        break

                if not key_flag:
                    # recherche de la table dans les noms d'entities déclarées
                    for key in map_entities_train.keys():
                        # pour chaque nom de table dans Khiops on cherche la
                        # table qui correspond dans la liste des entites
                        if key == name_table_logs:
                            key_flag = True
                            additional_table[
                                name_root + "`" + name_table_logs
                            ] = map_entities_train[key]
                            break

                if not key_flag:
                    print(
                        "Le nom de la table "
                        + name_table_logs
                        + " dans le dico "
                        + self.dictionary
                        + "ne correspond à aucune des tables "
                        "déclarées dans data_tables"
                    )
                    exit()

        # Dictionnaire root datamart : rajout des entities
        # Dictionnaire root datamart : rajout des tables logs et des tables
        #                              TableSelection

        if not self.mobile:
            # date de fin de logs selon la date de début de cible
            # (jour, (ou heure, ou min) precedent)
            if period_unit == "days":
                date_end = start_date - timedelta(days=1)
                date_start = date_end - timedelta(days=input_data_duration)
            elif period_unit == "hours":
                date_end = start_date - timedelta(hours=1)
                date_start = date_end - timedelta(hours=input_data_duration)
            elif period_unit == "minutes":
                date_end = start_date - timedelta(minutes=1)
                date_start = date_end - timedelta(minutes=input_data_duration)

        else:
            pass

        for dico in dico_domain.dictionaries:
            if dico.root:
                # Unused    Date    date_target        ;
                for var in dico.variables:
                    if var.name == name_var_date_target:
                        var.used = False
                        break

                if self.mobile:
                    # Unused    Numerical    delta_target_random        ;
                    var_delta = pk.Variable()

                    var_delta.name = "delta_target_random"
                    var_delta.type = "Numerical"
                    var_delta.used = False
                    dico.add_variable(var_delta)

                # Dictionnaire root datamart : rajout des entities
                for key in map_entities_train.keys():
                    # verifier si l'entity existe déjà
                    entity = False
                    for var in dico.variables:
                        if var.name == key:
                            entity = True
                            break
                    if not entity:
                        # Entity(key) key;
                        var_entities = pk.Variable()
                        var_entities.name = key
                        var_entities.type = "Entity(" + key + ")"
                        dico.add_variable(var_entities)

                # Dictionnaire root datamart : rajout des tables logs et des
                #                              tables TableSelection
                for (
                    name_table_logs,
                    my_timestamp,
                ) in map_tables_timestamp.items():
                    # verifier si la table existe déjà
                    table = False
                    for var in dico.variables:
                        if var.name == name_table_logs:
                            var.used = False
                            table = True
                            break
                    if not table:
                        # Unused    Table(logs)    logs        ;
                        var_logs = pk.Variable()
                        var_logs.name = name_table_logs
                        var_logs.type = "Table(" + name_table_logs + ")"
                        var_logs.used = False
                        dico.add_variable(var_logs)

                    var_logs_selection = pk.Variable()
                    var_logs_selection.name = name_table_logs + "Selection"
                    var_logs_selection.type = "Table(" + name_table_logs + ")"
                    var_logs_selection.used = True
                    dico.add_variable(var_logs_selection)

                    if not self.mobile:
                        if period_unit == "days":
                            var_logs_selection.rule = (
                                "TableSelection(" + name_table_logs + ", And( "
                                "GE( Sum(DiffDate( GetDate("
                                + my_timestamp
                                + '), AsDate("'
                                + date_start.strftime("%Y-%m-%d")
                                + '","YYYY-MM-DD")) , '
                                + str(model_gap)
                                + ") ,0), "
                                "LE( Sum(DiffDate( GetDate("
                                + my_timestamp
                                + '), AsDate("'
                                + date_end.strftime("%Y-%m-%d")
                                + '","YYYY-MM-DD")), '
                                + str(model_gap)
                                + ") ,0)))"
                            )
                        elif period_unit == "hours":
                            var_logs_selection.rule = (
                                "TableSelection(" + name_table_logs + ", And( "
                                "GE( Sum(DiffTimestamp( "
                                + my_timestamp
                                + ', AsTimestamp("'
                                + date_start.strftime("%Y-%m-%d %H:%M:%S")
                                + '","YYYY-MM-DD HH:MM:SS")) , '
                                "Product(3600, " + str(model_gap) + ")) ,0), "
                                "LE( Sum(DiffTimestamp( "
                                + my_timestamp
                                + ', AsTimestamp("'
                                + date_end.strftime("%Y-%m-%d %H:%M:%S")
                                + '","YYYY-MM-DD HH:MM:SS")), '
                                "Product(3600, " + str(model_gap) + ")) ,0)))"
                            )
                        elif period_unit == "minutes":
                            var_logs_selection.rule = (
                                "TableSelection(" + name_table_logs + ", And( "
                                "GE( Sum(DiffTimestamp( "
                                + my_timestamp
                                + ', AsTimestamp("'
                                + date_start.strftime("%Y-%m-%d %H:%M:%S")
                                + '","YYYY-MM-DD HH:MM:SS")) , '
                                "Product(60, " + str(model_gap) + ")) ,0), "
                                "LE( Sum(DiffTimestamp( "
                                + my_timestamp
                                + ', AsTimestamp("'
                                + date_end.strftime("%Y-%m-%d %H:%M:%S")
                                + '","YYYY-MM-DD HH:MM:SS")), '
                                "Product(60, " + str(model_gap) + ")) ,0)))"
                            )

                    else:
                        if period_unit == "days":
                            var_logs_selection.rule = (
                                "TableSelection(" + name_table_logs + ", And( "
                                "LE(delta_jours, Sum("
                                + str(model_gap)
                                + ", delta_target, "
                                + str(input_data_duration)
                                + ")), "
                                "GE(delta_jours, Sum("
                                + str(model_gap)
                                + ", delta_target))))"
                            )
                        elif period_unit == "hours":
                            var_logs_selection.rule = (
                                "TableSelection(" + name_table_logs + ", And( "
                                "LE(delta_jours, Product(Sum("
                                + str(model_gap)
                                + ", delta_target, "
                                + str(input_data_duration)
                                + "), 3600)), "
                                "GE(delta_jours, Product(Sum("
                                + str(model_gap)
                                + ", delta_target), 3600))))"
                            )
                        elif period_unit == "minutes":
                            var_logs_selection.rule = (
                                "TableSelection(" + name_table_logs + ", And( "
                                "LE(delta_jours, Product(Sum("
                                + str(model_gap)
                                + ", delta_target, "
                                + str(input_data_duration)
                                + "), 60)), "
                                "GE(delta_jours, Product(Sum("
                                + str(model_gap)
                                + ", delta_target), 60))))"
                            )

            else:  # elif dico_root == False:
                if self.mobile:
                    name_table_logs = dico.name
                    # recherche de la table dans les noms de tables déclarées
                    for key in map_tables.keys():
                        # pour chaque nom de table dans Khiops,
                        # on cherche la table qui correspond dans data_tables
                        if key == name_table_logs:
                            name_var_id_table = self.data_tables["tables"][
                                key
                            ]["key"]
                            # Unused    Entity(sampledatamart)    principal[line_id_sha]    ;
                            var_entity_root = pk.Variable()

                            var_entity_root.name = "principal"
                            var_entity_root.rule = (
                                "[" + name_var_id_table + "]"
                            )
                            var_entity_root.type = "Entity"
                            var_entity_root.object_type = name_root
                            var_entity_root.used = False
                            dico.add_variable(var_entity_root)

                            # Unused Numerical delta_target = GetValue(principal, delta_target_random);
                            var_delta2 = pk.Variable()

                            var_delta2.name = "delta_target"
                            var_delta2.type = "Numerical"
                            var_delta2.rule = (
                                "GetValue(principal, delta_target_random)"
                            )
                            var_delta2.used = False
                            dico.add_variable(var_delta2)

                            # Unused Numerical delta_jours = DiffDate(GetValueD(principal, date_souscription), GetDate(my_timestamp));
                            var_delta3 = pk.Variable()

                            var_delta3.name = "delta_jours"
                            var_delta3.type = "Numerical"
                            if period_unit == "days":
                                var_delta3.rule = (
                                    "DiffDate(GetValueD(principal, "
                                    + name_var_date_target
                                    + "), GetDate("
                                    + map_tables_timestamp[dico.name]
                                    + "))"
                                )
                            elif (
                                period_unit == "hours"
                                or period_unit == "minutes"
                            ):  # resultat de DiffTimestamp en secondes
                                var_delta3.rule = (
                                    "DiffTimestamp(GetValueTS(principal, "
                                    + name_var_date_target
                                    + "), "
                                    + map_tables_timestamp[dico.name]
                                    + ")"
                                )
                            var_delta3.used = False
                            dico.add_variable(var_delta3)

                            break

        rep, file = path.split(self.dictionary)
        if not self.mobile:
            dico_domain.export_khiops_dictionary_file(
                path.join(
                    rep, "dico_fixe_" + str(input_data_duration) + ".kdic"
                )
            )
        else:
            dico_domain.export_khiops_dictionary_file(
                path.join(
                    rep, "dico_mobile_" + str(input_data_duration) + ".kdic"
                )
            )

        return name_root, dico_domain, additional_table

    def _construct_datamarts_for_fit(
        self,
        path_file_train,
        format_timestamp_target,
    ):
        """Création d'un nouveau datamart à partir des datamarts mensuels.

        Pour chaque id, selon la date de l'événement, on récupère la ligne dans
        le bon datamart (datamart précédent le plus proche de la date de
        l'événement).
        """
        model_gap = self.temporal_parameters["model_gap"]
        period_unit = self.temporal_parameters["period_unit"]
        name_var_date_target = self.target_parameters["datetime"]
        nb_mois_datamarts = {}

        for key in self.data_tables["entities"].keys():
            nb_mois_datamarts[key] = len(self.data_tables["entities"][key])

        rep_train, name_file_train = path.split(path_file_train)
        df_train = pd.read_csv(
            path_file_train, sep=self.sep, encoding="ISO-8859-1"
        )

        dico_domain = pk.read_dictionary_file(self.dictionary)

        # creation de la liste des datetime disponibles dans la définition des
        # datamarts
        list_datamarts_datetime = creation_list_datamarts_datetime(
            self.data_tables, format_timestamp_target
        )

        # ajout de la reference du datamart correspondant à la date de cible
        # dans df_train
        df_train_ref = df_train.copy()

        if not self.mobile:
            if period_unit == "days":
                df_train_ref["ref_target"] = pd.DatetimeIndex(
                    df_train_ref[name_var_date_target]
                ) - np.timedelta64(1 + model_gap, "D")
            elif period_unit == "hours":
                df_train_ref["ref_target"] = pd.DatetimeIndex(
                    df_train_ref[name_var_date_target]
                ) - np.timedelta64(1 + model_gap, "h")
            elif period_unit == "minutes":
                df_train_ref["ref_target"] = pd.DatetimeIndex(
                    df_train_ref[name_var_date_target]
                ) - np.timedelta64(1 + model_gap, "m")

        else:
            if period_unit == "days":
                df_train_ref["ref_target"] = df_train_ref[
                    [self.target_parameters["datetime"], "delta_target_random"]
                ].apply(
                    lambda x: pd.to_datetime(
                        x[self.target_parameters["datetime"]],
                        format=format_timestamp_target,
                    )
                    - np.timedelta64(
                        1 + model_gap + x["delta_target_random"], "D"
                    ),
                    axis=1,
                )

            elif period_unit == "hours":
                df_train_ref["ref_target"] = df_train_ref[
                    [self.target_parameters["datetime"], "delta_target_random"]
                ].apply(
                    lambda x: pd.to_datetime(
                        x[self.target_parameters["datetime"]],
                        format=format_timestamp_target,
                    )
                    - np.timedelta64(
                        1 + model_gap + x["delta_target_random"], "h"
                    ),
                    axis=1,
                )

            elif period_unit == "minutes":
                df_train_ref["ref_target"] = df_train_ref[
                    [self.target_parameters["datetime"], "delta_target_random"]
                ].apply(
                    lambda x: pd.to_datetime(
                        x[self.target_parameters["datetime"]],
                        format=format_timestamp_target,
                    )
                    - np.timedelta64(
                        1 + model_gap + x["delta_target_random"], "m"
                    ),
                    axis=1,
                )

        # list_datamarts_datetime[0]
        df_train_ref["ref_entity"] = df_train_ref["ref_target"].apply(
            lambda x: list_datamarts_datetime[0]
            if x >= list_datamarts_datetime[0]
            else None
        )
        # list_datamarts_datetime[1]   ...
        for i in range(len(list_datamarts_datetime) - 1):
            df_train_ref["temp"] = df_train_ref[
                ["ref_target", "ref_entity"]
            ].apply(
                lambda x: list_datamarts_datetime[i + 1]
                if x["ref_target"] >= list_datamarts_datetime[i + 1]
                else x["ref_entity"],
                axis=1,
            )
            df_train_ref["ref_entity"] = df_train_ref["temp"]
        df_train_ref.drop(["ref_target", "temp"], axis=1)

        list_different_ref = (
            df_train_ref["ref_entity"].value_counts().index.tolist()
        )

        # si la période est sur un seul mois on prend le datamart correspondant
        if len(list_different_ref) == 1:
            map_entities_train = create_map_entities(
                self.data_tables, datetime_str=list_different_ref[0]
            )

        # si la période couvre plusieurs mois reconstruction du datamart à
        # partir des différents mois
        elif len(list_different_ref) > 1:
            # construction, pour chaque datamart (key),
            # d'un nouveau datamart à partir des datamarts mensuels
            map_entities_train = {}
            for key in self.data_tables["entities"].keys():
                # si le datamart existe déjà on ne le reconstruit pas
                datamart_train = key + "_" + name_file_train
                file_datamart_train = path.join(rep_train, datamart_train)

                try:
                    with open(file_datamart_train):
                        print(
                            "Le fichier '"
                            + file_datamart_train
                            + "' existe déjà"
                        )
                except IOError:
                    # liste des ids pour lesquels on va récupérer un datamart
                    # s'il existe
                    df_train_id = df_train[
                        self.data_tables["main_table"]["key"]
                    ]

                    # creation dataframe de départ, vide
                    df_union_sel_train = df_train_id.iloc[:0]

                    # lecture dictionnaire pour recuperation de la liste des
                    # variables (pour trier les variables du dataframe généré
                    # dans cet ordre)
                    list_var_datamart = []
                    key_flag = False
                    for dico in dico_domain.dictionaries:
                        if not dico.root:
                            if key == dico.name:
                                key_flag = True
                                for var in dico.variables:
                                    list_var_datamart.append(var.name)
                    if not key_flag:
                        print(
                            "La table "
                            + key
                            + " n'apparait pas dans le dictionnaire Khiops "
                            + self.dictionary
                        )
                        exit()

                    # parcours de toutes les value_ref, sélection des ids et
                    # pour ces ids récupération du datamart correspondant à la
                    # value_ref
                    for value_ref in list_different_ref:
                        # value_ref = value_ref.strftime(format_timestamp_target)

                        # selection des lignes de df_train pour chaque valeur
                        # ref_entity
                        df_sel_train = df_train_ref[
                            df_train_ref["ref_entity"] == value_ref
                        ]
                        df_sel_train = df_sel_train[
                            self.data_tables["main_table"]["key"]
                        ]

                        # recherche du datamart correspondant
                        len_datamart = len(self.data_tables["entities"][key])
                        for i in range(len_datamart):
                            if (
                                self.data_tables["entities"][key][i][
                                    "datetime"
                                ]
                                == value_ref
                            ):
                                datamart = self.data_tables["entities"][key][
                                    i
                                ]["file_name"]
                                break

                        exist(datamart)
                        dfdatamart = pd.read_csv(
                            datamart,
                            sep=self.sep,
                            encoding="ISO-8859-1",
                            dtype="unicode",
                        )

                        # jointure avec le datamart correspondant
                        df_sel_train = pd.merge(
                            df_sel_train,
                            dfdatamart,
                            how="left",
                            left_on=self.data_tables["main_table"]["key"],
                            right_on=self.data_tables["entities"][key][i][
                                "key"
                            ],
                        )

                        # concatenation avec le ref_entity précédent
                        df_union_sel_train = pd.concat(
                            [df_union_sel_train, df_sel_train],
                            ignore_index=True,
                        )

                    # concatenation avec les ids du fichier train
                    # (pour avoir tous les ids, y compris ceux sans datamart)
                    df_train_entity = pd.merge(
                        df_train_id,
                        df_union_sel_train,
                        how="left",
                        on=self.data_tables["main_table"]["key"],
                    )
                    df_train_entity = df_train_entity.reindex(
                        columns=list_var_datamart
                    )

                    # écriture du datamart ainsi constitué
                    df_train_entity.to_csv(
                        file_datamart_train, sep=self.sep, index=False
                    )
                    # df_train_entity = df_train_entity.sort_values
                    # (by = data_tables["entities"][key][i]['key'])

                map_entities_train[key] = file_datamart_train

        return map_entities_train

    def _add_date_ref(self, file, target_duration):
        """
        Ajout d'une date de référence dans les fichiers train et test pour la
        modélisation

            | - ajout de **date_ref** calculée en fonction de la
                **target_duration** (paramètre l)

            .. math:: date\_ref = date\_target - random(0, l-1)

            | - écriture du nouveau fichier
        """
        # extraction du nom du fichier et de l extension
        file_sans_ext, extension = parse_name_file(file)

        # ajout date_ref si le fichier n existe pas deja
        if not path.exists(
            file_sans_ext + "_target" + str(target_duration) + extension
        ):
            dico_domain = pk.read_dictionary_file(self.dictionary)

            for dico in dico_domain.dictionaries:
                if dico.root:
                    name_root = dico.name

                    # Numerical delta_target_random = Minus(Floor(Product(Random(), l)));
                    var_delta = pk.Variable()

                    var_delta.name = "delta_target_random"
                    var_delta.type = "Numerical"
                    var_delta.rule = (
                        "Floor(Product(Random(), "
                        + str(target_duration)
                        + "))"
                    )
                    var_delta.used = True
                    dico.add_variable(var_delta)

            pk.deploy_model(
                dico_domain,  # dictionary file path or domain
                name_root,  # Name of the dictionary to deploy
                path.join(file),
                path.join(
                    file_sans_ext
                    + "_target"
                    + str(target_duration)
                    + extension
                ),
                field_separator=self.sep,
                output_field_separator=self.sep,
            )

    def fit(self):
        """Modélisation"""
        name_var_date_target = self.target_parameters["datetime"]
        target = self.target_parameters["target"]

        period_unit = self.temporal_parameters["period_unit"]
        input_data_duration = self.temporal_parameters["input_data_duration"]
        start_date = self.temporal_parameters["start_date"]
        model_gap = self.temporal_parameters["model_gap"]

        # détection de format_timestamp_target
        format_timestamp_target = detect_format_timestamp(
            self.dictionary, name_var_date_target
        )

        # vérification de l'existence du fichier train
        file_target = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_target)
        file_train = path.join(rep, "train_" + file)
        exist(file_train)

        # extraction du nom du fichier et de l extension
        file_train_sans_ext, extension = parse_name_file(file_train)

        if self.mobile:
            start_date = ""
            target_duration = self.temporal_parameters["target_duration"]
            # ajout date_ref, creation d un fichier pour chaque valeur de
            # target_duration
            self._add_date_ref(file_train, target_duration)

            file_fit = (
                file_train_sans_ext
                + "_target"
                + str(target_duration)
                + extension
            )
            print("fichier train : " + file_fit)
        else:
            file_fit = file_train
            print("fichier train : " + file_fit)

        # vérification de l'existence de datamarts
        is_datamart = exist_datamart(self.data_tables)
        if is_datamart:
            map_entities_train = self._construct_datamarts_for_fit(
                file_fit,
                format_timestamp_target,
            )
        else:
            map_entities_train = {}

        print("dictionnaire :" + self.dictionary)
        rep_result = work_path(rep, self.mobile)

        # modification du dictionnaire à la volee pour sélection des logs - sc2
        (
            name_root,
            dico_domain,
            additional_table,
        ) = self._modif_selection_dico_khiops_for_fit(
            map_entities_train,
            name_var_date_target,
            start_date,
            period_unit,
            model_gap,
            input_data_duration,
        )

        # modelisation
        pk.train_predictor(
            dico_domain,  # dictionary file path or domain
            name_root,  # name of the table's dictionary
            file_fit,  # data table file path
            target,  # target variable name
            rep_result,
            sample_percentage=100,
            field_separator=self.sep,
            max_trees=0,
            additional_data_tables=additional_table,
            max_constructed_variables=(1000 * input_data_duration),
        )

    def _lecture_additional_data_tables_nodatamart(self, dico_domain):
        """
        Lecture du dictionnaire à la volée pour récupération des tables
        secondaires

            additional_table_modeling = {'SNB_sampledatamart`logs':repertoire_data + "S\_logs\_" + nom_cible + ".csv"}
        """
        # Dictionnaires logs : recuperation du nom des tables
        additional_table_modeling = {}

        # recuperation des path des tables et entities dans data_tables
        # ce ne sera pas le même ordre que le dico khiops

        map_tables = create_map_tables(self.data_tables)

        for dico in dico_domain.dictionaries:
            if dico.root:
                name_root = dico.name
                break

        for dico in dico_domain.dictionaries:
            if not dico.root:
                # suppression du prefixe 'SNB_' pour rechercher le nom dans
                # map_tables_entities
                name_table_logs = dico.name[4:]
                # pour chaque nom de table et entity dans Khiops on cherche la
                # table qui correspond dans data_tables pour récupérer le path
                for key in map_tables.keys():
                    key_flag = False

                    if key == name_table_logs:
                        key_flag = True
                        additional_table_modeling[
                            name_root + "`" + name_table_logs
                        ] = map_tables[key]
                        break

                if not key_flag:
                    print(
                        "Le nom de la table "
                        + name_table_logs
                        + " dans le dico ne correspond à aucune des tables "
                        "déclarées dans data_tables"
                    )
                    exit()

        return additional_table_modeling

    def _lecture_additional_data_tables_datamart(
        self, dico_domain, map_entities_datetime
    ):
        """
        Construction de *additional_table_modeling* à partir de
        *map_entities_datetime* et *map_table* (plus besoin de parcourir le dico)

            additional_table_modeling = {'SNB_sampledatamart`logs':repertoire_data + "S\_logs\_" + name_target + ".csv"}
        """

        additional_table_modeling = {}

        # recuperation des path des tables et entities dans data_tables
        # ce ne sera pas le même ordre que le dico khiops

        map_tables_entities = map_entities_datetime.copy()
        map_tables = create_map_tables(self.data_tables)
        map_tables_entities.update(map_tables)

        for dico in dico_domain.dictionaries:
            if dico.root:
                name_root = dico.name
                break

        for key in map_tables_entities.keys():
            # pour chaque nom de table dans data_tables on cherche la table ou
            # entity qui correspond dans Khiops pour récupérer le path
            for dico in dico_domain.dictionaries:
                if not dico.root:
                    # suppression du prefixe 'SNB_' pour rechercher le nom dans
                    # map_tables_entities
                    name_table_logs = dico.name[4:]
                    if name_table_logs in key:
                        # additional_table_modeling[name_root + '`' + name_table_logs] = map_tables_entities[key]
                        additional_table_modeling[
                            name_root + "`" + key
                        ] = map_tables_entities[key]
                        break

        return additional_table_modeling

    def _modif_selection_dico_khiops_for_depl_datamart(
        self, dico_domain, model_gap, period_unit
    ):
        """
        Modification du dictionnaire à la volée pour la préparation au déploiement
        Sauf pour la date de déploiement qui sera modifiée à chaque pas ou à chaque changement de datamart:
        # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0Selection =
        #               TableSelection(name_of_the_first_entity_table0, GE( Diff(DiffDate(AsDate("2000-01-01", "YYYY-MM-DD"),
        #               AsDate("2019-09-01", "YYYY-MM-DD")), Sum(7, .delta_target_random)), 0))		;
        """

        for dico in dico_domain.dictionaries:
            if dico.root:
                # remarque: dans khiops on ne peut pas donner à la table le nom "key + datetime" (à cause des tirets)
                # error: Variable `name_of_the_first_entity_table_2019-09-01` : Incorrect name for a native variable of type Entity: must not contain back-quote
                # solution : on numérote et on stocke dans un dictionnaire python si besoin de retrouver le numéro
                map_key_datetime = (
                    {}
                )  # pour retrouver le numéro associé map_key_datetime[key][datetime] = numéro
                map_entities_datetime = (
                    {}
                )  # pour période mobile, crée le dictionnaire de toutes les entities {key, name_file_with_datetime} pour tous les datetime

                for key in self.data_tables["entities"].keys():
                    keySNB = "SNB_" + key
                    # ligne à supprimer du dico
                    # Unused	Entity(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table		;
                    dico.remove_variable(key)

                    # lignes à rajouter
                    # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0		;
                    # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table1		;
                    # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table2		;
                    len_datamart = len(self.data_tables["entities"][key])
                    for i in range(len_datamart):
                        map_key_datetime[
                            key,
                            str(
                                self.data_tables["entities"][key][i][
                                    "datetime"
                                ]
                            ),
                        ] = str(i)
                        # Unused	Entity(keySNB) key;
                        var_entity = pk.Variable()
                        var_entity.name = key + str(i)
                        var_entity.type = "Table(" + keySNB + ")"
                        var_entity.used = False
                        dico.add_variable(var_entity)
                        # récupération dans un dico python du nom de la table créée pour khiops et du fichier correspondant
                        file_entity = self.data_tables["entities"][key][i][
                            "file_name"
                        ]
                        map_entities_datetime[var_entity.name] = file_entity

                    # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0Selection =
                    # TableSelection(name_of_the_first_entity_table0, GE( Diff(DiffDate(AsDate("2019-09-15", "YYYY-MM-DD"),
                    # AsDate("2019-09-01", "YYYY-MM-DD")), Sum(7, .delta_target_random)), 0))		;
                    for i in range(len_datamart):
                        var_table = pk.Variable()
                        var_table.name = key + str(i) + "Selection"
                        var_table.type = "Table(" + keySNB + ")"
                        datetime_ref = str(
                            self.data_tables["entities"][key][i]["datetime"]
                        )
                        if period_unit == "days":
                            var_table.rule = (
                                "TableSelection("
                                + key
                                + str(i)
                                + ", GE( Diff(DiffDate(AsDate("
                                '"2000-01-01", "YYYY-MM-DD"), AsDate('
                                + datetime_ref
                                + ', "YYYY-MM-DD")), '
                                "Sum("
                                + str(model_gap)
                                + ", .delta_target_random)), 0))"
                            )
                        elif period_unit == "hours":
                            var_table.rule = (
                                "TableSelection("
                                + key
                                + str(i)
                                + ", GE( Diff(DiffTimestamp(AsTimestamp("
                                '"2000-01-01 00:00:00", "YYYY-MM-DD HH:MM:SS"), AsTimestamp("'
                                + datetime_ref
                                + '", "YYYY-MM-DD HH:MM:SS")), '
                                "Product(Sum("
                                + str(model_gap)
                                + ", .delta_target_random), 3600)), 0))"
                            )
                        elif period_unit == "minutes":
                            var_table.rule = (
                                "TableSelection("
                                + key
                                + str(i)
                                + ", GE( Diff(DiffTimestamp(AsTimestamp("
                                '"2000-01-01 00:00:00", "YYYY-MM-DD HH:MM:SS"), AsTimestamp("'
                                + datetime_ref
                                + '", "YYYY-MM-DD HH:MM:SS")), '
                                "Product(Sum("
                                + str(model_gap)
                                + ", .delta_target_random), 60)), 0))"
                            )
                        var_table.used = False
                        dico.add_variable(var_table)

                    # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_tableUnion	 =
                    # TableUnion(name_of_the_first_entity_table2Selection, name_of_the_first_entity_table1Selection,
                    # name_of_the_first_entity_table0Selection)	;

                    var_table = pk.Variable()
                    var_table.name = key + "Union"
                    var_table.type = "Table(" + keySNB + ")"
                    rule = "TableUnion("
                    for i in reversed(range(len_datamart)):
                        rule += key + str(i) + "Selection,"
                    rule = rule[:-1] + ")"
                    var_table.rule = rule
                    var_table.used = False
                    dico.add_variable(var_table)

                    # Unused	Entity(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table	 =
                    # TableAt(name_of_the_first_entity_tableUnion, 1)	;

                    var_entity = pk.Variable()
                    var_entity.name = key
                    var_entity.type = "Entity(" + keySNB + ")"
                    var_entity.rule = "TableAt(" + key + "Union, 1)"
                    var_entity.used = False
                    dico.add_variable(var_entity)
                break

        return dico_domain, map_entities_datetime

    def _modif_selection_dico_khiops_datetime_depl_nodatamart_mobile(
        self, dico_domain, my_date, format_timestamp_target, period_unit
    ):
        """
        Modification du dictionnaire à la volée pour le déploiement

        .. note:: La date de déploiement est modifiée à chaque pas :
            Dans chacune des tables
            # Unused    Numerical    delta_jours     = DiffDate(AsDate("2020-09-01", "YYYY-MM-DD"), GetDate(my_timestamp))    ;
        """
        modif = False
        for dico in dico_domain.dictionaries:
            if not dico.root:
                name_table_logs = dico.name
                for key in self.data_tables["tables"].keys():
                    keySNB = "SNB_" + key
                    if keySNB == name_table_logs:
                        # récupération du nom des variables Timestamp
                        my_timestamp = self.data_tables["tables"][key][
                            "datetime"
                        ]
                        find = False
                        for var in dico.variables:
                            if var.type == "Timestamp":
                                if var.name == my_timestamp:
                                    find = True
                                    break
                        if not find:
                            print(
                                "la table '"
                                + key
                                + "' doit comporter la variable datetime : "
                                + my_timestamp
                            )
                            exit()

                        # Unused    Numerical    delta_jours     = DiffDate(AsDate("2020-09-01", "YYYY-MM-DD"), GetDate(my_timestamp))    ;
                        for var in dico.variables:
                            if var.name == "delta_jours":
                                if period_unit == "days":
                                    var.rule = (
                                        'DiffDate(AsDate("'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD"), GetDate('
                                        + my_timestamp
                                        + "))"
                                    )
                                    modif = True
                                elif (
                                    period_unit == "hours"
                                    or period_unit == "minutes"
                                ):
                                    var.rule = (
                                        'DiffTimestamp(AsTimestamp("'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD HH:MM:SS"), '
                                        + my_timestamp
                                        + ")"
                                    )
                                    modif = True
                                break
        if not modif:
            print(
                "attention la date de déploiement n'est pas prise en compte, vérifiez les données"
            )
            exit()

        return dico_domain

    def _modif_selection_dico_khiops_datetime_depl_datamart_mobile(
        self,
        dico_domain,
        my_date,
        format_timestamp_target,
        period_unit,
        model_gap,
    ):
        """
        Modification du dictionnaire à la volée pour le déploiement

        ..note:: La date de déploiement est modifiée à chaque pas :

            Dans la table root pour chacun des datamarts selection des datetime
            # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0Selection =
            #               TableSelection(name_of_the_first_entity_table0, GE( Diff(DiffDate(AsDate(my_date, "YYYY-MM-DD"),
            #               AsDate("2019-09-01", "YYYY-MM-DD")), Sum(7, .delta_target_random)), 0))		;
        """

        modif = False
        for dico in dico_domain.dictionaries:
            if dico.root:
                # Rajout des variables dans toutes les entities
                for key in self.data_tables["entities"].keys():
                    len_datamart = len(self.data_tables["entities"][key])
                    for i in range(len_datamart):
                        # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0Selection =
                        #               TableSelection(name_of_the_first_entity_table0, GE( Diff(DiffDate(AsDate(my_date, "YYYY-MM-DD"),
                        #               AsDate("2019-09-01", "YYYY-MM-DD")), Sum(7, .delta_target_random)), 0))		;
                        for var in dico.variables:
                            if var.name == (key + str(i) + "Selection"):
                                datetime_ref = str(
                                    self.data_tables["entities"][key][i][
                                        "datetime"
                                    ]
                                )
                                if period_unit == "days":
                                    var.rule = (
                                        "TableSelection("
                                        + key
                                        + str(i)
                                        + ", GE( Diff(DiffDate(AsDate("
                                        '"'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD"), AsDate("'
                                        + datetime_ref
                                        + '", "YYYY-MM-DD")), '
                                        "Sum("
                                        + str(model_gap)
                                        + ", .delta_target_random)), 0))"
                                    )
                                    modif = True
                                elif period_unit == "hours":
                                    var.rule = (
                                        "TableSelection("
                                        + key
                                        + str(i)
                                        + ", GE( Diff(DiffTimestamp(AsTimestamp("
                                        '"'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD HH:MM:SS"), AsTimestamp("'
                                        + datetime_ref
                                        + '", "YYYY-MM-DD HH:MM:SS")), '
                                        "Product(Sum("
                                        + str(model_gap)
                                        + ", .delta_target_random), 3600)), 0))"
                                    )
                                    modif = True
                                elif period_unit == "minutes":
                                    var.rule = (
                                        "TableSelection("
                                        + key
                                        + str(i)
                                        + ", GE( Diff(DiffTimestamp(AsTimestamp("
                                        '"'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD HH:MM:SS"), AsTimestamp("'
                                        + datetime_ref
                                        + '", "YYYY-MM-DD HH:MM:SS")), '
                                        "Product(Sum("
                                        + str(model_gap)
                                        + ", .delta_target_random), 60)), 0))"
                                    )
                                    modif = True
        if not modif:
            print(
                "attention la date de déploiement n'est pas prise en compte, vérifiez les données"
            )
            exit()

        return dico_domain

    def _modif_selection_dico_khiops_datetime_depl_datamart_fixe(
        self, dico_domain, my_date, format_timestamp_target, period_unit
    ):
        """
        Modification du dictionnaire à la volée pour le déploiement

        .. note:: La date de déploiement est modifiée à chaque pas :

            Dans la table root pour chacun des datamarts selectin des datetime
            # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0Selection =
            #               TableSelection(name_of_the_first_entity_table0, GE( DiffDate(AsDate(my_date, "YYYY-MM-DD"),
            #               AsDate("2019-09-01", "YYYY-MM-DD")), 0))		;
        """

        modif = False
        for dico in dico_domain.dictionaries:
            if dico.root:
                # rajout des variables dans toutes les entities
                for key in self.data_tables["entities"].keys():
                    len_datamart = len(self.data_tables["entities"][key])
                    for i in range(len_datamart):
                        # Unused	Table(SNB_name_of_the_first_entity_table)	name_of_the_first_entity_table0Selection =
                        #               TableSelection(name_of_the_first_entity_table0, GE( DiffDate(AsDate(my_date, "YYYY-MM-DD"),
                        #               AsDate("2019-09-01", "YYYY-MM-DD")), 0))		;
                        for var in dico.variables:
                            if var.name == (key + str(i) + "Selection"):
                                datetime_ref = str(
                                    self.data_tables["entities"][key][i][
                                        "datetime"
                                    ]
                                )
                                if period_unit == "days":
                                    var.rule = (
                                        "TableSelection("
                                        + key
                                        + str(i)
                                        + ", GE( DiffDate(AsDate("
                                        '"'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD"), AsDate("'
                                        + datetime_ref
                                        + '", "YYYY-MM-DD")), '
                                        "0))"
                                    )
                                    modif = True
                                elif (
                                    period_unit == "hours"
                                    or period_unit == "minutes"
                                ):
                                    var.rule = (
                                        "TableSelection("
                                        + key
                                        + str(i)
                                        + ", GE( DiffTimestamp(AsTimestamp("
                                        '"'
                                        + my_date.strftime(
                                            format_timestamp_target
                                        )
                                        + '", "YYYY-MM-DD HH:MM:SS"), AsTimestamp("'
                                        + datetime_ref
                                        + '", "YYYY-MM-DD HH:MM:SS")), '
                                        "0))"
                                    )
                                    modif = True
        if not modif:
            print(
                "attention la date de déploiement n'est pas prise en compte, vérifiez les données"
            )
            exit()

        return dico_domain

    def predict(self):
        """Déploiement sur la période period_nb par pas de period_unit"""

        name_var_id = self.data_tables["main_table"]["key"]

        name_var_date_target = self.target_parameters["datetime"]
        target = self.target_parameters["target"]
        main_target_modality = self.target_parameters["main_target_modality"]

        period_unit = self.temporal_parameters["period_unit"]
        model_gap = self.temporal_parameters["model_gap"]
        target_duration = self.temporal_parameters["target_duration"]
        period_nb = self.temporal_parameters["period_nb"]
        try:
            depl_start_date = self.temporal_parameters["depl_start_date"]
        except KeyError:
            depl_start_date = self.temporal_parameters["start_date"]

        # vérification de l'existence du fichier test
        file_target = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_target)
        file_test = path.join(rep, "test_" + file)
        exist(file_test)

        print("fichier test : " + file_test)
        rep_result = work_path(rep, self.mobile)

        # extraction du nom du fichier et de l extension
        file_test_without_ext, extension = parse_name_file(file_test)

        # vérification de l'existence de datamarts
        is_datamart = exist_datamart(self.data_tables)

        # detection de format_timestamp_target
        dico_ref = path.join(rep_result, "Modeling.kdic")
        format_timestamp_target = detect_format_timestamp(
            dico_ref, name_var_date_target
        )

        # modification du dictionnaire Modeling.kdic
        dico_domain = pk.read_dictionary_file(dico_ref)
        for dico in dico_domain.dictionaries:
            if dico.root:
                snb_root_dictionary = dico
                name_root = dico.name

        snb_root_dictionary.use_all_variables(False)
        for var in snb_root_dictionary.variables:
            if var.name in {
                name_var_id,
                "Prob" + target + str(main_target_modality),
            }:
                var.used = True

        if self.mobile:
            target_duration = self.temporal_parameters["target_duration"]
            # ajout date_ref, creation d un fichier pour chaque valeur de
            # target_duration
            self._add_date_ref(file_test, target_duration)

            file_depl = (
                file_test_without_ext
                + "_target"
                + str(target_duration)
                + extension
            )
            print("fichier deploiement : " + file_depl)
        else:
            file_depl = file_test

        # Déploiement sur period_nb
        """
        le modèle est déployé de depl_start_date à depl_start_date + period_nb
        déploiement à la date depl_start_date -> transfer_1 
        (datamart ayant le datetime correspondant)
        par pas de period_unit on regarde si le datetime existe déjà, 
        sinon on effectue un nouveau déploiement transfer_2...
        """

        # creation de la liste des datetime disponibles dans la définition des
        # datamarts
        if is_datamart:
            # creation de la liste des datetime disponibles dans la définition
            # des datamarts
            list_datamarts_datetime = creation_list_datamarts_datetime(
                self.data_tables, format_timestamp_target
            )

        # initialisation date de début de déploiement
        depl_start_date = depl_start_date
        depl_date = depl_start_date
        if not self.mobile:
            list_depl = []
            len_list_depl = len(list_depl)

        # conversion du model_gap en timedelta
        if period_unit == "days":
            gap = timedelta(days=model_gap)
        elif period_unit == "hours":
            gap = timedelta(hours=model_gap)
        elif period_unit == "minutes":
            gap = timedelta(minutes=model_gap)

        # lecture des tables secondaires
        if is_datamart:
            # on a besoin de tous les datamarts
            if not self.mobile:
                model_gap = 0
            (
                dico_domain,
                map_entities_datetime,
            ) = self._modif_selection_dico_khiops_for_depl_datamart(
                dico_domain, model_gap, period_unit
            )
            additional_table_modeling = (
                self._lecture_additional_data_tables_datamart(
                    dico_domain, map_entities_datetime
                )
            )
        else:
            additional_table_modeling = (
                self._lecture_additional_data_tables_nodatamart(dico_domain)
            )

        # fixe
        if not self.mobile:
            for step in range(period_nb):
                # pour chaque pas on regarde si cela crée un nouvel élément
                # dans la liste
                datetime_depl = ""
                name_depl = "transfer"
                if is_datamart:
                    for datamart_datetime in list_datamarts_datetime:
                        if (depl_date - gap) >= datamart_datetime:
                            datetime_depl = datamart_datetime
                            name_depl = "transfer_" + str(datetime_depl)
                    if datetime_depl == "":
                        print(
                            "les datetime des tables entities doivent couvrir les dates de déploiements, or la date '"
                            + str(depl_date)
                            + "' n'est pas couverte"
                        )
                        exit()
                # print(str(depl_date) + ' -> ' + name_depl)
                if name_depl not in list_depl:
                    list_depl.append(name_depl)
                    num_depl = len(list_depl)

                    # on regarde si c'est un nouvel élément dans la liste
                    # -> si oui un nouveau déploiement
                    if num_depl > len_list_depl:
                        # mise à jour de la taille de la liste
                        len_list_depl = num_depl

                        # modification du dictionnaire Modeling.kdic
                        if is_datamart:
                            my_date = datetime_depl
                            dico_domain = self._modif_selection_dico_khiops_datetime_depl_datamart_fixe(
                                dico_domain,
                                my_date,
                                format_timestamp_target,
                                period_unit,
                            )
                        # dico_domain.export_khiops_dictionary_file(path.join(rep_result, "TransferDatabase", 'dico_' + str(num_depl) + '.kdic'))

                        # Transfert
                        pk.deploy_model(
                            dico_domain,  # dictionary file path or domain
                            name_root,  # name of the modeling dictionary
                            file_depl,  # data table file
                            path.join(
                                rep_result,
                                "TransferDatabase",
                                "transfer_" + str(num_depl) + ".csv",
                            ),  # output data table file
                            field_separator=self.sep,
                            additional_data_tables=additional_table_modeling,
                        )

                # on décale d'une unité period_unit
                if period_unit == "days":
                    depl_date = depl_date + timedelta(days=1)
                    # dates = pd.date_range(start_date, end_date - timedelta(days=1), freq='D')
                elif period_unit == "hours":
                    depl_date = depl_date + timedelta(hours=1)
                    # dates = pd.date_range(start_date, end_date - timedelta(hours=1), freq='H')
                elif period_unit == "minutes":
                    depl_date = depl_date + timedelta(minutes=1)
                    # dates = pd.date_range(start_date, end_date - timedelta(minutes=1), freq='min')

            print(
                "--> nombre de déploiements " + str(len_list_depl) + " -> OK"
            )

        # mobile
        else:
            for step in range(period_nb):
                num_depl = step
                # modification du dictionnaire Modeling.kdic
                if is_datamart:
                    dico_domain = self._modif_selection_dico_khiops_datetime_depl_datamart_mobile(
                        dico_domain,
                        depl_date,
                        format_timestamp_target,
                        period_unit,
                        model_gap,
                    )
                dico_domain = self._modif_selection_dico_khiops_datetime_depl_nodatamart_mobile(
                    dico_domain,
                    depl_date,
                    format_timestamp_target,
                    period_unit,
                )
                # dico_domain.export_khiops_dictionary_file(path.join(rep_result, "TransferDatabase", 'dico_' + str(num_depl) + '.kdic'))

                # transfert
                pk.deploy_model(
                    dico_domain,  # dictionary file path or domain
                    name_root,  # name of the modeling dictionary
                    file_depl,  # data table file
                    path.join(
                        rep_result,
                        "TransferDatabase",
                        "transfer_" + str(num_depl) + ".csv",
                    ),  # output data table file
                    field_separator=self.sep,
                    additional_data_tables=additional_table_modeling,
                )

                # on décale d'une unité period_unit
                if period_unit == "days":
                    depl_date = depl_date + timedelta(days=1)
                    # dates = pd.date_range(start_date, end_date - timedelta(days=1), freq='D')
                elif period_unit == "hours":
                    depl_date = depl_date + timedelta(hours=1)
                    # dates = pd.date_range(start_date, end_date - timedelta(hours=1), freq='H')
                elif period_unit == "minutes":
                    depl_date = depl_date + timedelta(minutes=1)
                    # dates = pd.date_range(start_date, end_date - timedelta(minutes=1), freq='min')

            print("--> nombre de déploiements " + str(period_nb) + " -> OK")

    def _constitution_target_period_unit(
        self,
        name_var_id,
        name_file_test,
        target,
        main_target_modality,
        name_var_date_target,
        start_date,
        format_timestamp_target,
        period_unit,
        nb_targets,
    ):
        """Constitution du fichier cible journalier"""

        df_target = pd.read_csv(name_file_test, sep=self.sep)
        df_target = df_target[[name_var_id, target, name_var_date_target]]

        # si period_unit hours ou minutes : decoupage de la cible en periode
        # heure ou minutes (si days rien a faire)
        if period_unit == "hours":
            decoupage = "H"
        elif period_unit == "minutes":
            decoupage = "min"
        if period_unit == "hours" or period_unit == "minutes":
            # passage de la date en datetime
            df_target[name_var_date_target] = pd.to_datetime(
                df_target[name_var_date_target], format=format_timestamp_target
            )
            # arrondi a l heure ou minute inferieure
            df_target[name_var_date_target] = df_target[
                name_var_date_target
            ].dt.floor(decoupage)
            # on repasse la date en objet pour calculer date * cible
            # df_target[name_var_date_target] = df_target[name_var_date_target].astype(str)

        # creation de la liste des dates
        start_date = start_date
        if period_unit == "days":
            end_date = start_date + timedelta(days=nb_targets)
            dates = pd.date_range(
                start_date, end_date - timedelta(days=1), freq="D"
            )
        elif period_unit == "hours":
            end_date = start_date + timedelta(hours=nb_targets)
            dates = pd.date_range(
                start_date, end_date - timedelta(hours=1), freq="H"
            )
        elif period_unit == "minutes":
            end_date = start_date + timedelta(minutes=nb_targets)
            dates = pd.date_range(
                start_date, end_date - timedelta(minutes=1), freq="min"
            )

        def is_my_date(row):
            if row[name_var_date_target] == str(
                date.strftime(format_timestamp_target)
            ):
                if str(row[target]) == str(main_target_modality):
                    return 1
                else:
                    return 0
            else:
                return 0

        # creation d'une colonne de cible pour chaque date
        for date in dates:
            name_var = target + str(date)
            df_target[name_var] = df_target.apply(
                lambda row: is_my_date(row), axis=1
            )

        df_target.drop(
            [
                target,
            ],
            axis=1,
            inplace=True,
        )
        df_target.drop(
            [
                name_var_date_target,
            ],
            axis=1,
            inplace=True,
        )

        if period_unit == "days":
            df_target.columns = df_target.columns.str.replace(" 00:00:00", "")

        return df_target

    def _concat_transfert_creation_pivot(
        self,
        df_res,
        rep_result,
        name_var_id,
        target,
        main_target_modality,
        start_date,
        format_timestamp_target,
        period_unit,
        period_nb,
        is_datamart,
    ):
        """Concaténation des 2 dataframes cibles et scores"""

        # creation de la liste des datetime disponibles dans la définition des
        # datamarts
        if is_datamart:
            list_datamarts_datetime = creation_list_datamarts_datetime(
                self.data_tables, format_timestamp_target
            )

        # lecture des fichiers transfer
        my_date = start_date
        list_depl = []

        for step in range(period_nb):
            if not self.mobile:
                # recherche du fichier transfer correspondant à my_date
                datetime_depl = ""
                name_depl = "transfer"
                if is_datamart:
                    for datamart_datetime in list_datamarts_datetime:
                        if my_date >= datamart_datetime:
                            datetime_depl = datamart_datetime
                            name_depl = "transfer_" + str(datetime_depl)
                    if datetime_depl == "":
                        print(
                            "les datetime des tables entities doivent couvrir les dates de déploiements, or la date '"
                            + str(my_date)
                            + "' n'est pas couverte"
                        )
                        exit()

                if name_depl not in list_depl:
                    list_depl.append(name_depl)

            # récupération du nom du fichier transfer
            if self.mobile:
                num_depl = step
            else:
                num_depl = len(list_depl)
            file_transfer = path.join(
                rep_result,
                "TransferDatabase",
                "transfer_" + str(num_depl) + ".csv",
            )
            df = pd.read_csv(file_transfer, sep="\t")
            df = df[[name_var_id, "Prob" + target + str(main_target_modality)]]

            df.columns = [
                name_var_id,
                "score_" + my_date.strftime(format_timestamp_target),
            ]
            df_res = pd.merge(df_res, df, how="inner", on=name_var_id)
            print("score_" + my_date.strftime(format_timestamp_target))

            if period_unit == "days":
                my_date += timedelta(days=1)
            elif period_unit == "hours":
                my_date += timedelta(hours=1)
            elif period_unit == "minutes":
                my_date += timedelta(minutes=1)

        return df_res

    @staticmethod
    def _evaluation_reactif_df(param_eval, df_to_eval, file_to_write):
        """Exécution de l'évaluation en réactif timeevalscore.py"""
        eval_react = ReactiveEvalScore(param_eval)
        eval_react.eval_score_df(param_eval, df_to_eval, latency=1)
        eval_react.write_report_file(file_to_write + ".xls")
        print(
            "Ecriture du fichier de resultats de l evaluation reactif : "
            + file_to_write
            + ".xls"
        )
        eval_react.write_report_file_json(file_to_write + ".json")
        print(
            "Ecriture du fichier de resultats de l evaluation reactif : "
            + file_to_write
            + ".json"
        )

    @staticmethod
    def _evaluation_proactif_df(param_eval, df_to_eval, file_to_write):
        """Exécution de l'évaluation en proactif timeevalscore.py"""
        eval_pro = ProactiveEvalScore(param_eval)
        eval_pro.eval_score_df(param_eval, df_to_eval, latency=7)
        eval_pro.write_report_file(file_to_write + ".xls")
        print(
            "Ecriture du fichier de resultats de l evaluation proactif : "
            + file_to_write
            + ".xls"
        )
        eval_pro.write_report_file_json(file_to_write + ".json")
        print(
            "Ecriture du fichier de resultats de l evaluation proactif : "
            + file_to_write
            + ".json"
        )

    def evaluate(self):
        """
        Evaluation
            | - constitution de la table des scores et cibles journaliers
            | - calcul des indicateurs avec timeevalscore
        """

        name_var_id = self.data_tables["main_table"]["key"]

        name_var_date_target = self.target_parameters["datetime"]
        target = self.target_parameters["target"]
        main_target_modality = self.target_parameters["main_target_modality"]
        default_target_modality = self.target_parameters[
            "default_target_modality"
        ]

        period_unit = self.temporal_parameters["period_unit"]
        start_date = self.temporal_parameters["start_date"]
        target_duration = self.temporal_parameters["target_duration"]
        period_nb = self.temporal_parameters["period_nb"]
        nb_targets = period_nb + target_duration

        # vérification de l'existence du fichier test
        file_target = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_target)
        file_test = path.join(rep, "test_" + file)
        exist(file_test)
        print("fichier test : " + file_test)

        is_datamart = exist_datamart(self.data_tables)
        rep_result = work_path(rep, self.mobile)

        # detection de format_timestamp_target
        format_timestamp_target = detect_format_timestamp(
            self.dictionary, name_var_date_target
        )

        # constitution du fichier cible par period_unit
        df_target = self._constitution_target_period_unit(
            name_var_id,
            file_test,
            target,
            main_target_modality,
            name_var_date_target,
            start_date,
            format_timestamp_target,
            period_unit,
            nb_targets,
        )
        # concatenation des fichiers transferts et creation de la table pivot
        df_res = df_target
        df_res = self._concat_transfert_creation_pivot(
            df_res,
            rep_result,
            name_var_id,
            target,
            main_target_modality,
            start_date,
            format_timestamp_target,
            period_unit,
            period_nb,
            is_datamart,
        )

        table_pivot = "table_pivot_depl" + str(period_nb) + ".csv"
        df_res.to_csv(path.join(rep_result, table_pivot), sep=";", index=False)

        # evaluations reactives et proactives

        i_bin = 20  # liste des pct de target analyse
        i_eval_duration = min(
            period_nb, 30
        )  # duree en nombre de jours analyse
        i_nb_target = (
            nb_targets  # nombre de colonnes de cibles dans le fichier
        )
        i_nb_score = period_nb  # nombre de colonnes de scores dans le fichier
        id_position = 0  # position colonne de l id
        param_eval_reac = (
            i_bin,
            i_eval_duration,
            i_nb_target,
            i_nb_score,
            id_position,
        )
        Dataset._evaluation_reactif_df(
            param_eval_reac,
            df_res,
            path.join(rep_result, "eval_" + table_pivot + "_reactif"),
        )

        # list_bin_target=[0.1,0.2,0.3,0.4] # liste des pct de target analyse
        list_bin_target = [
            x * 0.1 for x in range(1, 10)
        ]  # liste des pct de target analyse
        param_eval_pro = (
            list_bin_target,
            i_eval_duration,
            i_nb_target,
            i_nb_score,
            id_position,
        )
        Dataset._evaluation_proactif_df(
            param_eval_pro,
            df_res,
            path.join(rep_result, "eval_" + table_pivot + "_proactif"),
        )

    def plot(self):
        """
        Représentations graphiques de courbes par top scores
        des deux métriques : précision et rappel
        """

        period_nb = self.temporal_parameters["period_nb"]

        # récupération du répertoire principal
        file_target = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_target)
        rep_result = work_path(rep, self.mobile)

        for type_eval in ["reactif", "proactif"]:
            table_pivot = "table_pivot_depl" + str(period_nb) + ".csv"
            eval_table_pivot_json_file = path.join(
                rep_result, "eval_" + table_pivot + "_" + type_eval + ".json"
            )
            with open(eval_table_pivot_json_file, "r") as json_file:
                data = json.load(json_file)

            for metric in ["precision", "rappel"]:
                dict_json_file = data[metric]
                for key, value in dict_json_file.items():
                    dict_json_file[key] = float(value)

                x, y = zip(
                    *dict_json_file.items()
                )  # unpack a list of pairs into two tuples
                plt.plot(x, y)
                plt.title(type_eval + " - " + metric)
                plt.xlabel("Top scores")
                plt.ylabel(metric)

                plt.savefig(
                    path.join(rep_result, type_eval + "_" + metric + ".png")
                )
                plt.show()

            try:
                dict_json_file_gain = data["gain"]
            except KeyError:
                dict_json_file_gain = {}
