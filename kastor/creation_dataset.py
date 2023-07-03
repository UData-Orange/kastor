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
"""
import numpy as np
import pandas as pd
import random

from datetime import datetime
from os import path
from time import process_time

from kastor.util import detect_format_timestamp, exist


class dataset(object):
    """Classe pour générer les datasets train et test correspondant aux
    intervalles définis par l'utilisateur.

    Parameters
    ----------
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

                "name_of_the_second_entity_table" : ?

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

                "sample2_logs_churn_xdsl":

                    | "file_name": file name with path,
                    | "key": name of the id variable,
                    | "datetime": name of the datetime variable

                "name of the second table":

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

            | "time_unit" : time unit for fit and for predict,
            | "input_data_duration" : # L # durée de prise en compte des logs
            | "model_gap" : model gap,
            | "target_duration" : # l # profondeur d'observation de la cible
            | "target_start_date": target start date,
            | "target_end_date": target end date, optional
            | "nb_scores": durée de la période de scores

            .. warning:: "input_data_duration", "target_duration" et
                "nb_scores" sont à exprimer dans la même unité que *time_unit*.

    dict_param_data
        Paramètres liés aux données

        .. note:: Contenu de **dict_param_data**

            repertoire_data : str
                Le répertoire où se trouvent les données
            dictionary : str
                Le nom du dictionnaire Khiops décrivant les données
            name_var_id : str
                Le nom de la variable identifiant
            sep : str
                Séparateur des fichiers de données

            .. warning:: "name_var_id" et "sep" doivent être les mêmes pour
                toutes les tables.

    dict_param_target
        Paramètres liés à la cible (dans la table principale)

        .. note:: Contenu de **dict_param_target**

            name_file_target : str
                Le nom du fichier comportant la table principale
            target : str
                Le nom de la variable cible (le même pour toutes les tables)
            main_target_modality : str ou int
                Le nom de la modalité cible (en général 1)
            default_target_modality : str ou int
                Le nom de la modalité autre (en général 0)
            target : str
                Le nom de la variable comportant l'horodatage de l'événement
            target_start_date : str
                La date de début de la période cible
            target_end_date : str
                La date de fin de la période cible
            format_timestamp_target : str
                Le format python sous lequel sont fournis les timestamps

            .. warning:: "target" doit être le même pour toutes les tables.

    dict_param_logs
        Paramètres liés aux données des tables secondaires

        .. note:: Contenu de **dict_param_logs**

            name_file_logs : list[str]
                La ou les tables secondaires de type logs
            name_var_date_log : list[str]
                Le ou les noms des variables comportant l'horodatage des logs
            format_timestamp_log : str
                Le format python sous lequel sont fournis les timestamps

            .. warning:: "format_timestamp_log" doit être le même pour toutes
                les tables.
    """

    def __init__(
        self,
        dictionary,
        data_tables,
        target_parameters,
        temporal_parameters,
        sep="\t",
    ):
        self.dictionary = dictionary
        self.data_tables = data_tables
        self.target_parameters = target_parameters
        self.temporal_parameters = temporal_parameters
        self.sep = sep

    def generate_timestamp_target(self):
        """Génération des timestamps aléatoires pour la cible 0

            Sélection des cibles = 1 dont les dates sont comprises dans l'intervalle spécifié
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

        # Detection de format_timestamp_target
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
            self.temporal_parameters["target_start_date"]
        )  # datetime.strptime(self.temporal_parameters["target_start_date"],format_timestamp_target)
        end_date_target = pd.Timestamp(
            self.temporal_parameters["target_end_date"]
        )  # datetime.strptime(self.temporal_parameters["target_end_date"],format_timestamp_target)
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

            # Detection de format_timestamp_log
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

            # verification des intervalles de dates pour tirage des dates pour les cibles=0 dans les logs
            # si la periode des logs recouvre celle des cibles tirage dans les logs, sinon tirage dans les cibles
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
            # tirage parmi les logs --> creer le fichier de logs sur la même période que la cible
            # on prend ici le fichier de logs en memoire, soit le dernier de la liste

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
        df_target,
        pourcentage_train=0.7,
        effectif_target=0,
        effectif_no_target=0,
    ):
        """Génération des datasets de train et test

        Parameters
        ----------
        df_target : dataframe
            Le dataframe généré par la fonction ``generate_timestamp_target()``
        pourcentage_train : optional, default 0.7
            Proportion des données pour le dataset de train
        effectif_target : optional, default 0
            Effectif à attribuer au dataset de train, à utiliser avec
            effectif_no_target, si specifié pourcentage_train est ignoré
        effectif_no_target : optional, default 0
            Effectif à attribuer au dataset de test, à utiliser avec
            effectif_target, si specifié pourcentage_train est ignoré

        Returns
        -------
        df_train : fichier csv
        df_test : fichier csv
            Les fichiers train et test sont écrits dans le répertoire des
            données
        """

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
            # pourcentage_train = kwargs.get("pourcentage_train", 0.7)
            nb_test = round(len(df_target) * (1 - pourcentage_train))
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
