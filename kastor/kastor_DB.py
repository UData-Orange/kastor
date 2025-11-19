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
**Automatisation de la chaine de production de scores pour des données temporelles**

.. image:: ./_static/contexte.jpg
   :align: center

Cette librairie python permet de dérouler la chaine de production de scores
de bout en bout. Les traitements Khiops sont automatisés via pykhiops, allant
de la production du modèle à la restitution de scores par périodes predéfinies
ainsi qu’à leur évaluation. Les modules définis sont les suivants :

    **train_test_split** : création du dataset
        | Sélection des cibles comprises dans un intervalle temporel à spécifier
        | Tirage aléatoire d'une date pour les non cibles et les cibles hors
            intervalle (tirage parmi les dates de logs si l'intervalle correspond,
            parmi les dates de cible sinon)
        | Constitution des deux datasets train et test
        
    **generate_dataset** : à partir d'un nouveau dataset
        | Sélection des cibles comprises dans un intervalle temporel à spécifier
        | Tirage aléatoire d'une date pour les non cibles et les cibles hors
            intervalle (tirage parmi les dates de logs si l'intervalle correspond,
            parmi les dates de cible sinon)

    **fit** : modélisation sur période fixe et/ou sur période mobile

    **predict** : test, backtesting ou déploiement, sur période fixe et/ou sur 
    période mobile
        | Sur une période fixe définie (par exemple 1 mois en marketing)
        | Sur une période mobile : les traitements sont effectués sur une période
            mobile définie en paramètre (par exemple 1, 7 ou 15 jours en marketing)

    **evaluate** : deux métriques d'évaluation
        | précision
        | rappel

    **plot** : tracé, par déciles, des courbes précision et rappel

"""
import numpy as np
import pandas as pd
import random
import datetime
from datetime import timedelta
from os import path
from khiops import core as kh
from time import process_time
from sys import exit
from time import time
import copy

from kastor._util import (
    compare_data_tables_with_dictionary,
    compare_dictionary_with_data_tables,
    detect_format_timestamp,
    exist,
    parse_name_file,
    double_transform,
)


class GenerateDataset:
    """Classe pour générer les datasets dans l'intervalle
    défini par l'utilisateur.

    Parameters
    ----------
    dictionary : `str`
        Dictionnaire Khiops décrivant les données :
        `dictionary = "my_file_path/my_dico.kdic"`

    data_table : `dict`
        Définition des données et de leurs liens key et datetime

        .. warning::
            | Les noms des tables `"main_table"`,
            | `"name_of_the_first_entity_table"`,
                `"name_of_the_second_entity_table"`, ...,
            | `"name_of_the_first_table"`,
                `"name_of_the_second_table"`,...
            | doivent correspondre aux noms renseignés dans le dictionnaire Khiops

        ``"main_table"`` : `dict`
            la table principale comportant la cible horodatée

                ``"name_main_table"`` : `str`
                    name of the main table
                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable

        ``"entities"`` : `dict`
            les Datamarts

            .. warning:: "datetime" : dernier jour non compris,
                doit être exprimé dans la même unité que
                *target_parameters["datetime"]*

            ``"name_of_the_first_entity_table"`` : `list` of `dict`

            liste de dictionnaires correspondant à chacun des datamarts
            caractérisé par son datetime : [`first_entity_date1`,
            `first_entity_date2`, `first_entity_date3`, ...]

            `first_entity_date1` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    date de référence du datamart, par ex
                    `datetime.date(2019, 9, 1)`

            `first_entity_date2` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    date de référence du datamart, par ex
                    `datetime.date(2019, 10, 1)`

            `first_entity_date3` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    date de référence du datamart, par ex
                    `datetime.date(2019, 11, 1)`

            ``"name_of_the_second_entity_table"`` : `list` of `dict`

            [`second_entity_date1`, `second_entity_date2`,
            `second_entity_date3`, ...]

            `second_entity_date1` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    date de référence du datamart, par ex
                    `datetime.date(2019, 9, 1)`

            `second_entity_date2` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    date de référence du datamart, par ex
                    `datetime.date(2019, 10, 1)`

            `second_entity_date3` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    date de référence du datamart, par ex
                    `datetime.date(2019, 11, 1)`

        ``"tables"`` : `dict`
            les tables secondaires de type tables de logs

            ``"name_of_the_first_table"`` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    name of the datetime variable

            ``"name_of_the_second_table"`` : `dict`

                ``"file_name"`` : `str`
                    file name with path
                ``"key"`` : `str`
                    name of the id variable
                ``"datetime"`` : `datetime` or `date`
                    name of the datetime variable

    temporal_parameters : `dict`
        Paramètres temporels de l'étude

            ``"period_unit"`` : `str`
                time unit for fit and for predict : ``"days"``, ``"hours"``
                or ``"minutes"``
            ``"input_data_duration"`` : `int`
                durée de prise en compte des logs (Tx)
            ``"model_gap"`` : `int`
                gap du modèle (correspond au gap métier) (tau)
            ``"target_duration"`` : `int`
                profondeur d'observation de la cible (Ty)
            ``"start_date"`` : `datetime` or `date`, optional
                start date for *fit* or for *predict*
            ``"end_date"`` : `datetime` or `date`, optional
                end date for *fit* or for *predict*
            ``"period_nb"`` : `int`, default `1`
                durée de la période de scores, si spécifié ``"end_date"`` est ignoré

            .. warning:: **input_data_duration**, **model_gap** et **target_duration**
                sont à exprimer dans la même unité que *period_unit*.

            .. warning:: **start_date** et **end_date**
                sont à exprimer dans la même unité que *target_parameters["datetime"]*

    target_parameters : `dict`
        Paramètres liés à la cible (dans la table principale)

            ``"datetime"`` : `str`
                name of the datetime variable
            ``"target"`` : `str`
                name of the target variable
            ``"main_target_modality"`` : `str`, optional
                main target modality
            ``"default_target_modality"`` : `str`, optional
                default target name

    sep : `str`, default `"\\\\t"`
        Le séparateur de données doit être le même pour toutes les tables

    mobile : `bool`, default `True`
        | `mobile=True` par défaut : étude en période mobile
        | `mobile=False` : étude en période fixe
    """

    def __init__(
        self,
        dictionary,
        data_tables,
        target_parameters,
        temporal_parameters,
        sep="\t",
        mobile=True,
        path_suffix = "",
    ):
        self.dictionary = dictionary
        self.data_tables = data_tables
        self.target_parameters = target_parameters
        self.temporal_parameters = temporal_parameters
        self.sep = sep
        self.mobile = mobile
        self.path_suffix = path_suffix
        self.dataset_type = "traintest"


    def _generate_timestamp_target(self, df_target):
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
            
        name_dico_main = self.data_tables["main_table"]["name_main_table"]
        (
            format_timestamp_target_khiops,
            format_timestamp_target_python,
            timestamp_target_type,
        ) = detect_format_timestamp(
            self.dictionary,
            name_dico_main,
            name_var_date_target,
        )
        
        name_target = self.target_parameters["target"]
        
        try:    
            main_target_modality = (
                self.target_parameters["main_target_modality"]
            )
        except KeyError:
            main_target_modality = 1
        try:
            default_target_modality = (
                self.target_parameters["default_target_modality"]
            )
        except KeyError:
            default_target_modality = 0
        
        date_target_not_null = df_target[
            ~df_target[name_var_date_target].isnull()
        ]
        date_target_not_null = date_target_not_null[
            name_var_date_target
        ]
        ts_date_target_not_null = pd.to_datetime(
            date_target_not_null, format=format_timestamp_target_python
        )

        ts_date_target_min = min(ts_date_target_not_null)
        ts_date_target_max = max(ts_date_target_not_null)
        print("timestamp min: " + str(ts_date_target_min))
        print("timestamp max: " + str(ts_date_target_max))
        
        # verification des intervalles de dates :
        # les bornes doivent être comprises dans les données fournies 
        # entre start_date et start_date + period_nb - 1 + target_duration
        try:
            start_date = pd.Timestamp(
                self.temporal_parameters["start_date"]
            )
        except KeyError:
            start_date = ts_date_target_min          
                
        if start_date < ts_date_target_min:
            raise ValueError(
                "la date de debut de cible specifiee "
                + str(start_date)
                + " est inferieure a la date minimale des donnees "
                + str(ts_date_target_min)
            )
                
        period_unit = self.temporal_parameters["period_unit"]    
        #target_duration = self.temporal_parameters["target_duration"]
        
        no_end_date = True
        try:
            period_nb = self.temporal_parameters["period_nb"]
        except KeyError:
            try:
                end_date = pd.Timestamp(self.temporal_parameters["end_date"])
                no_end_date = False   
            except KeyError:
                period_nb = 1

        # on décale de period_nb unités period_unit
        # et de target_duration unités period_unit pour l'évaluation
        if no_end_date:
            # ex: start_date = datetime.date(2019,10,1)
            # period_nb = 1 => end_date = datetime.date(2019,10,2)
            # tirage du timestamp entre (2019,10,1,0,0,0) et (2019,10,2,0,0,0)
            if period_unit == "days":
                end_date = start_date + timedelta(days= period_nb - 1)
                    
            elif period_unit == "hours":
                end_date = start_date + timedelta(hours= period_nb - 1)
                    
            elif period_unit == "minutes":
                end_date = start_date + timedelta(minutes= period_nb - 1)
              
        # chargement des logs
        self.tirage = "logs"
        for key in self.data_tables["tables"].keys():
            file_log = self.data_tables["tables"][key]["file_name"]
            exist(file_log)
            df_logs = pd.read_csv(
                file_log, sep=self.sep, encoding="ISO-8859-1"
            )

            # detection de format_timestamp_log
            try:
                name_var_timestamp_log = self.data_tables["tables"][key][
                    "datetime"
                ]
            except KeyError:
                break
            
            name_dico_logs = key
            (
                format_timestamp_log_khiops,
                format_timestamp_log_python,
                timestamp_log_type,
            ) = detect_format_timestamp(
                self.dictionary,
                name_dico_logs,
                name_var_timestamp_log,
            )

            print(
                "\n\n" + file_log + " --> " + str(len(df_logs)) + " lignes\n"
            )

            ts_date_log = pd.to_datetime(
                df_logs[self.data_tables["tables"][key]["datetime"]],
                format=format_timestamp_log_python,
            )

            ts_date_log_min = ts_date_log.min(skipna=True)
            ts_date_log_max = ts_date_log.max(skipna=True)   
            print("timestamp min: " + str(ts_date_log_min))
            print("timestamp max: " + str(ts_date_log_max))

            # verification des intervalles de dates pour tirage des dates pour
            # les cibles=0 dans les logs
            # si la periode des logs recouvre celle des cibles tirage dans les
            # logs, sinon tirage dans les cibles
            if (start_date < ts_date_log_min) or (
                end_date > ts_date_log_max
            ):
                self.tirage = "cible"

        # constitution du fichier cible

        # on garde toutes les cibles = 1 comprises entre start_date et end_date
        df_target_1 = df_target[df_target[name_target] == main_target_modality]
        df_target_1 = df_target_1[
            (
                pd.to_datetime(
                    df_target_1[name_var_date_target],
                    format=format_timestamp_target_python,
                )
                >= start_date
            )
            & (
                pd.to_datetime(
                    df_target_1[name_var_date_target],
                    format=format_timestamp_target_python,
                )
                <= end_date
            )
        ]

        nb_target_1 = len(df_target_1)
        print(
            "\nNombre de cibles avec modalite "
            + str(main_target_modality)
            + " comprises entre le "
            + str(start_date) + " et le " + str(end_date) + " : "
            + str(nb_target_1)
        )
        """
        # on complète avec les cibles = 0 et cible = 1 hors bornes
        df_target_0 = df_target.drop(df_target_1.index)
        nb_target_0 = len(df_target_0)
        df_target_0[name_target] = default_target_modality
        df_target_0[name_var_date_target] = np.nan
        """

        # on complète avec les cibles = 0 et cible = 1 hors bornes
        df_target_0 = df_target.drop(df_target_1.index)

        # plusieurs traitements à effectuer sur les cibles  = 0 et cible = 1 
        # hors bornes
        # si cible 0 effacer les datetime qui sont hors bornes
        df_target_0.loc[
            (
                df_target_0[name_target] == default_target_modality
             ) &
            ((
                pd.to_datetime(
                    df_target_0[name_var_date_target],
                    format = format_timestamp_target_python) < start_date
                )
            | (
                pd.to_datetime(
                    df_target_0[name_var_date_target],
                    format = format_timestamp_target_python) > end_date
                )
            ),
            name_var_date_target] = np.nan

        #si cible 1 et datetime < start_date_cible supprimer la ligne
        df_target_0.drop(df_target_0[
            (
                df_target_0[name_target] == main_target_modality
                ) &
            (
                pd.to_datetime(
                    df_target_0[name_var_date_target],
                    format = format_timestamp_target_python) < start_date
                )
            ].index,
            inplace=True
            )

        # si cible 1 et datetime > end_date_cible effacer datetime
        df_target_0.loc[
            (
                df_target_0[name_target] == main_target_modality
                ) &
            (
                pd.to_datetime(
                    df_target_0[name_var_date_target],
                    format = format_timestamp_target_python) > end_date
                ) ,
            name_var_date_target] = np.nan

        # tirage d'une date si datetime non renseigné
        # mettre cible à 0
        df_target_0[name_target] = default_target_modality

        # compter les datetime np.nan et boucler seulement sur eux
        # separer dfcible0 : datetime valides vs datetime vides (np.nan)
        df_target_0_nan = df_target_0[
            df_target_0[name_var_date_target].isnull()
            ]
        nb_target_0_nan = len(df_target_0_nan)

        # tirage aléatoire d'une date de cible pour les cibles = 0
        if nb_target_0_nan > 0:
            # separer dfcible0 : datetime valides vs datetime vides (np.nan)
            df_target_0_valid = df_target_0.drop(df_target_0_nan.index)
            df_target_0_nan = df_target_0_nan.reset_index(drop=True)

            if self.tirage == "logs":
                # tirage parmi les logs, soit creer le fichier de logs sur la
                # même période que la cible
                # on prend ici le fichier de logs en memoire,
                # soit le dernier de la liste qui a un datetime

                df_date_logs = df_logs[
                    self.data_tables["tables"][name_dico_logs]["datetime"]
                    ]
                df_date_for_target = df_date_logs.loc[
                    (
                        pd.to_datetime(
                            df_date_logs,
                            format=format_timestamp_log_python
                            )
                        >= start_date
                    )
                    & (
                        pd.to_datetime(
                            df_date_logs,
                            format=format_timestamp_log_python
                            )
                        <= end_date
                    )
                ]

            else:
                # tirage parmi les cibles
                df_date_for_target = df_target_1[name_var_date_target]

            list_index = df_date_for_target.index

            # boucle sur toutes les lignes pour affecter une date
            random.seed(666)
            t = process_time()
            if self.tirage == "logs":
                for n in range(nb_target_0_nan):
                    index = random.choice(list_index)
                    ts_str = df_date_for_target[index]
                    try:
                        ts = datetime.datetime.strptime(
                            ts_str,
                            format_timestamp_log_python
                            ).\
                            strftime(format_timestamp_target_python)
                    except ValueError:  # format timestamp avec micro-secondes
                        ts = datetime.datetime.strptime(
                            ts_str,
                            format_timestamp_log_python + ".%f"
                            ).\
                            strftime(format_timestamp_target_python)
                    df_target_0_nan.loc[n,name_var_date_target] = ts

            else:
                for n in range(nb_target_0_nan):
                    index = random.choice(list_index)
                    ts_str = df_date_for_target[index]
                    df_target_0_nan.loc[ n,name_var_date_target] = ts_str

            print(
                "\nDuree d execution de l'affectation d une date aleatoire"
                + " aux cibles "
                + str(default_target_modality)
                + " : "
                + str(round(process_time() - t))
                + "s"
            )
            # --> 388.4241736256332 = 6,5 minutes

            # on regroupe avec les cibles = 0 ayant un datetime valide
            df_target_0 = pd.concat(
                [df_target_0_valid,df_target_0_nan],
                ignore_index=True
                )

        # concatenation des cibles = 0 et cibles = 1
        df_target = pd.concat([df_target_1, df_target_0], ignore_index=True)
        df_target = df_target.sort_values(
            by=self.data_tables["main_table"]["key"]
        )
        df_target = df_target.reset_index(drop=True)

        # double traitement des dates pour transformer les éventuels formats
        # sur 1 chiffre en 2 chiffres (ex 2019/1/3 -> 2019/01/03)
        # sinon le mapping ne se fait pas lorsque l'on construit les cibles
        # pour la table pivot
        df_target[name_var_date_target] = double_transform(
            df_target[name_var_date_target],
            format_timestamp_target_python
            )

        return df_target

    def train_test_split(
        self,
        train_percentage=0.7,
        train_length=0,
        test_length=0,
        prefix="",
        #nicolas voisine
        # rajouter prefixe parametre optionnel default prefix=""
    ):
        """Génération des datasets de train et test

        Parameters
        ----------
        train_percentage : `float`, default `0.7`
            Proportion des données pour le dataset de train
        train_length :  `ìnt`, default `0`
            Effectif à attribuer au dataset de train, à utiliser avec
            test_length, si specifié train_percentage est ignoré
        test_length : `int`, default `0`
            Effectif à attribuer au dataset de test, à utiliser avec
            train_length, si specifié train_percentage est ignoré
        prefix : `str`, default `""`
            Préfixe au nom des fichiers

        Returns
        -------
        Les fichiers train et test sont écrits dans le répertoire des données
        """

        # pour savoir si fit/predict (train/test) ou predict (deploy)
        # info indispensable pour connaitre les intervalles de dates
        # pour le tirage aléatoire des dates
        
        debut = time()
        # vérification de la cohérence des tables déclarées entre data_tables 
        # et le dictionnaire khiops
        compare_data_tables_with_dictionary(
            self.dictionary, self.data_tables, self.target_parameters
        )
        compare_dictionary_with_data_tables(
            self.dictionary, self.data_tables, self.target_parameters
        )

        # chargement du fichier cible
        file_target = self.data_tables["main_table"]["file_name"]
        exist(file_target)

        df_target = pd.read_csv(
            file_target, sep=self.sep, encoding="ISO-8859-1"
        )

        print(file_target + " --> " + str(len(df_target)) + " lignes\n")
        
        try:
            main_target_modality = (
                self.target_parameters["main_target_modality"]
            )
        except KeyError:
            main_target_modality = 1
        
        # tirage aléatoire des dates :
        # en deploy seulement si la cible est renseignée
        # en train/test dans tous les cas
        if self.dataset_type == "traintest":
            name_target = self.target_parameters["target"]
            try:
                target = df_target[name_target]
                
            except KeyError:
                print(
                    "Erreur -> La variable '"
                    + name_target
                    + "' n'existe pas dans le fichier "
                    + file_target
                )
                exit()
                
            else:     
                print(df_target.groupby([target]).count())
                print("\n")
                df_target = self._generate_timestamp_target(df_target)
            
        elif self.dataset_type == "backtesting":            
            name_target = self.target_parameters["target"]
            try:
                target = df_target[name_target]
            
            except KeyError:
                self.dataset_type = "depl"
                #df_target = self._generate_timestamp_without_target(df_target)
                
            else:
                print(df_target.groupby([name_target]).count())
                print("\n")
                df_target = self._generate_timestamp_target(df_target)
        
        if (train_length > 0) & (test_length > 0):
            df_test = df_target.sample(
                n=test_length, random_state=666, replace=False
            ).sort_index(axis=0)
            df_remaining = df_target.drop(df_test.index)
            df_train = df_remaining.sample(
                n=train_length, random_state=1906, replace=False
            ).sort_index(axis=0)

        elif train_length > 0:
            df_train = df_target.sample(
                n=train_length, random_state=1906, replace=False
            ).sort_index(axis=0)
        
        elif test_length > 0:
            df_test = df_target.sample(
                n=test_length, random_state=666, replace=False
            ).sort_index(axis=0)
        
        else:
            test_length = round(len(df_target) * (1 - train_percentage))
            df_test = df_target.sample(
                n=test_length, random_state=666, replace=False
            ).sort_index(axis=0)
            df_remaining = df_target.drop(df_test.index)
            df_train = df_remaining

        rep, file = path.split(file_target)
        name_file, extension = parse_name_file(file)

        # écriture des fichiers train et test
        if prefix != "":
            prefix = prefix + "_" 

        # dictionnaire khiops pour le tri
        dico_domain = kh.read_dictionary_file(self.dictionary)
        name_dico_main = self.data_tables["main_table"]["name_main_table"]

        # tri khiops
        def kh_sort(name_df_ns, name_df):
            kh.sort_data_table(
                dico_domain, 
                name_dico_main, 
                name_df_ns, 
                name_df, 
                field_separator=self.sep, 
                output_field_separator=self.sep,
            )
    
        # backtesting
        if self.dataset_type == "backtesting":           
            # écriture du fichier avant tri
            name_df_depl_ns = path.join(
                    rep, 
                    str(prefix) + name_file + "_" + self.dataset_type  + '_ns'
                    + extension,
                )
            df_test.to_csv(name_df_depl_ns, sep=self.sep, index=False)
            
            # nombre de cibles
            df_test_1 = df_test[df_test[name_target] == main_target_modality]
            nb_cibles_test_1 = len(df_test_1)
            
            # tri et écriture du fichier via khiops
            name_df_depl = path.join(
                    rep, 
                    str(prefix) + name_file + "_" + self.dataset_type 
                    + extension,
                )
            kh_sort(name_df_depl_ns, name_df_depl)

            print(
                "\nfichier " + self.dataset_type + " : " 
                + str(len(df_test)) + " ids, "
                + str(nb_cibles_test_1) + " cibles avec modalité "
                + str(main_target_modality)
                +"\nécriture du fichier " + self.dataset_type + " : " 
                + name_df_depl
            )
            # copie de la structure           
            ST_DB_depl = copy.deepcopy(self.data_tables)
            ST_DB_depl["main_table"]["file_name"] = name_df_depl

            duree = time()-debut
            print("\ndurée d'exécution : " +str(timedelta(seconds=duree)))
    
            return ST_DB_depl
        
        # deploiement
        elif self.dataset_type == "depl":
           # écriture du fichier avant tri
           name_df_depl_ns = path.join(
                   rep, 
                   str(prefix) + name_file + "_" + self.dataset_type  + '_ns'
                   + extension,
               )
           df_test.to_csv(name_df_depl_ns, sep=self.sep, index=False)
           
           # tri et écriture du fichier via khiops
           name_df_depl = path.join(
                   rep, 
                   str(prefix) + name_file + "_" + self.dataset_type 
                   + extension,
               )
           kh_sort(name_df_depl_ns, name_df_depl)

           print(
               "\nfichier " + self.dataset_type + " : " 
               + str(len(df_test)) + " ids "
               +"\nécriture du fichier " + self.dataset_type + " : " 
               + name_df_depl
           )
           # copie de la structure           
           ST_DB_depl = copy.deepcopy(self.data_tables)
           ST_DB_depl["main_table"]["file_name"] = name_df_depl

           duree = time()-debut
           print("\ndurée d'exécution : " +str(timedelta(seconds=duree)))
   
           return ST_DB_depl

        # train/test
        elif self.dataset_type == "traintest":
            try:
                name_df_train_ns = path.join(
                        rep, str(prefix) + name_file + "_train_ns" + extension
                    )
                df_train.to_csv(name_df_train_ns, sep=self.sep, index=False)

                # nombre de cibles
                df_train_1 = (
                    df_train[df_train[name_target] == main_target_modality]
                )
                nb_cibles_train_1 = len(df_train_1)
                
            except NameError :
                pass
                
            else:
                # tri et écriture du fichier via khiops
                name_df_train = path.join(
                        rep, str(prefix) + name_file + "_train" + extension
                    )
                kh_sort(name_df_train_ns, name_df_train)
                
                print(
                    "\nfichier train : " + str(len(df_train)) + " ids, "
                    + str(nb_cibles_train_1) + " cibles avec modalité "
                    + str(main_target_modality)
                    + "\nécriture du fichier train : " + name_df_train
                )
    
            try :
                name_df_test_ns = path.join(
                        rep, str(prefix) + name_file + "_test_ns" + extension
                    )
                df_test.to_csv(name_df_test_ns, sep=self.sep, index=False)
                
            except NameError :
                pass
                
            else:
                # nombre de cibles
                df_test_1 = (
                    df_test[df_test[name_target] == main_target_modality]
                )
                nb_cibles_test_1 = len(df_test_1)
                
                # tri et écriture du fichier via khiops
                name_df_test = path.join(
                        rep, str(prefix) + name_file + "_test" + extension
                    )
                kh_sort(name_df_test_ns, name_df_test)
                
                print(
                    "\nfichier test : " + str(len(df_test)) + " ids, "
                    + str(nb_cibles_test_1) + " cibles avec modalité "
                    + str(main_target_modality)
                    + "\nécriture du fichier test : " + name_df_test)
                
            # copie des structures
            ST_DB_train = copy.deepcopy(self.data_tables)
            ST_DB_test = copy.deepcopy(self.data_tables)
            try:
                ST_DB_train["main_table"]["file_name"] = name_df_train
            except NameError:
                pass
            try:
                ST_DB_test["main_table"]["file_name"] = name_df_test
            except NameError:
                pass    
            # nicolas voisine
            # creer les database struture de train et test
            #   - mettre dans "main_table" dans des 2 strutures
            #       name_df_train et name_df_test
            # cree les fichier train et test de la table principale
            #   - le nom c'est <prefix>_nom de la table principale d'origine
            #           _"train/test".extension de la table d'origine
            #   - name_df_train et name_df_test
            return ST_DB_train, ST_DB_test
        
        duree = time()-debut
        print("\ndurée d'exécution : " +str(timedelta(seconds=duree)))
            

    def generate_dataset(
        self,
        back_percentage=1,
        back_length=0,
        prefix="",
    ):
        """Génération du dataset de backtesting

        Parameters
        ----------
        back_percentage : `float`, default `1`
            Proportion des données pour le dataset de backtesting
        back_length :  `ìnt`, default `0`
            Effectif à attribuer au dataset de backtesting, si specifié
            back_percentage est ignoré
        prefix : `str`, default `""`
            Préfixe au nom du fichier

        Returns
        -------
        Le fichier de backtesting est écrit dans le répertoire des données
        """
        
        # pour savoir si fit/predict (train/test) ou predict (backtesting)
        # info indispensable pour connaitre les intervalles de dates
        # pour le tirage aléatoire des dates
        
        self.dataset_type = "backtesting"
        
        ST_DB_test = self.train_test_split(
                train_percentage = 1 - back_percentage,
                test_length=back_length,
                prefix=prefix,
            )

        return ST_DB_test
        