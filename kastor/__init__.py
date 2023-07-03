######################################################################################
# Copyright (c) 2023 Orange - All Rights Reserved                             #
# * This software is the confidential and proprietary information of Orange.         #
# * You shall not disclose such Restricted Information and shall use it only in      #
#   accordance with the terms of the license agreement you entered into with Orange  #
#   named the "Kastor - Python Library Evaluation License".                          #
# * Unauthorized copying of this file, via any medium is strictly prohibited.        #
# * See the "LICENSE.md" file for more details.                                      #
######################################################################################
""" Déclaration des paramètres dans Python

# dictionnaire : dictionnaire Khiops décrivant les données
dictionnaire = dictionary Khiops file name with path (kdic file)

# data_table : définition des données et de leurs liens key et datetime
data_tables = {
   # nom de la table principale avec la cible horodatée
   "main_table": {
        "name_main_table": name of the main table (same as in khiops dictionary),
        "file_name": file name with path,
        "key": name of the id variable
        },
   # datamarts
   "entities": {
        "name_of_the_first_entity_table": [{  # NV c'est une liste de fichiers datamarts caractérisés par leur datetime
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": "2019-09-01" # dernier jour non compris, a exprimer dans la meme unite que format_timestamp_cible
            },
            {
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": "2019-10-01"
            },
            {
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": "2019-11-01"
            }],
        "name_of_the_second_entity_table" : [{
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": "2019-09-01"
            },
            {
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": "2019-10-01"

            },
            {
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": "2019-11-01"
            }]
        },
   # tables de logs
   "tables": {
        "sample2_logs_churn_xdsl": {
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": name of the datetime variable
            },
        "name of the second table": {
            "file_name": file name with path,
            "key": name of the id variable,
            "datetime": name of the datetime variable
            }
        }
    }

# Paramètres liés à la cible (dans la table principale)
target_parameters = {
    "datetime": name of the datetime variable,
    "target": name of the target variable,
    "main_target_modality": main target modality,  # optionnel
    "default_target_modality": default target name  # optionnel
    }

# Paramètres temporels de l'étude
temporal_parameters = {
    "time_unit" : time unit for fit and for predict, # La fréquence de prédiction et de déploiement : journalier : "days", heure : "hours", minute : "minutes"
    "input_data_duration" : input data duration, # L # durée de prise en compte des logs, à exprimer dans la même unité que time_unit
    "model_gap" : model gap,
    "target_duration" : target duration for mobile prediction, # l # profondeur d'observation de la cible à exprimer dans la même unité que time_unit (ex:1, 7, ou 15)
    "target_start_date": target start date,
    "target_end_date": target end date, # optionnel
    "depl_start_date": depl_start_date, # optionnel, target_start_date si non renseigné
    "nb_scores": nb_scores  # durée de la période de scores, à exprimer dans la même unité que time_unit
    }

# Paramètres optionnels (avec leur valeur par défaut)
sep = '\t' # Séparateur des fichiers de données, le même séparateur pour toutes les tables
format_timestamp_target = "%Y-%m-%d" # Le format python sous lequel sont fournis les timestamps
format_timestamp_log = "%Y-%m-%d %H:%M:%S" # Le format python sous lequel sont fournis les timestamps (le même pour toutes les tables)
nb_targets = nb_scores + target_duration # durée de la période de cibles, à exprimer dans la même unité que frequence_obs_cible

"""

""" Déclaration des paramètres pour Sphinx
    Paramètres
    ----------
    * dictionnaire (\ *str*\ ) : dictionary Khiops file name with path (kdic file)

    * data_table : définition des données et de leurs liens key et datetime
    data_tables = {
       # nom de la table principale avec la cible horodatée
       "main_table": {
            "name_main_table": name of the main table (same as in khiops dictionary),
            "file_name": file name with path,
            "key": name of the id variable
            },
       # datamarts
       "entities": {
            "name_of_the_first_entity_table": [{  # NV c'est une liste de fichiers datamarts caractérisés par leur datetime
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": "2019-09-01" # dernier jour non compris, a exprimer dans la meme unite que format_timestamp_cible
                },
                {
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": "2019-10-01"
                },
                {
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": "2019-11-01"
                }],
            "name_of_the_second_entity_table" : [{
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": "2019-09-01"
                },
                {
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": "2019-10-01"

                },
                {
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": "2019-11-01"
                }]
            },
       # tables de logs
       "tables": {
            "sample2_logs_churn_xdsl": {
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": name of the datetime variable
                },
            "name of the second table": {
                "file_name": file name with path,
                "key": name of the id variable,
                "datetime": name of the datetime variable
                }
            }
        }

    * Paramètres liés à la cible (dans la table principale)
    target_parameters = {
        "datetime": name of the datetime variable,
        "target": name of the target variable,
        "main_target_modality": main target modality,  # optionnel
        "default_target_modality": default target name  # optionnel
        }

    * Paramètres temporels de l'étude
    temporal_parameters = {
        "time_unit" : time unit for fit and for predict, # La fréquence de prédiction et de déploiement : journalier : "days", heure : "hours", minute : "minutes"
        "input_data_duration" : input data duration, # L # durée de prise en compte des logs, à exprimer dans la même unité que time_unit
        "model_gap" : model gap,
        "target_duration" : target duration for mobile prediction, # l # profondeur d'observation de la cible à exprimer dans la même unité que time_unit (ex:1, 7, ou 15)
        "target_start_date": target start date,
        "target_end_date": target end date, # optionnel
        "depl_start_date": depl_start_date, # optionnel, target_start_date si non renseigné
        "nb_scores": nb_scores  # durée de la période de scores, à exprimer dans la même unité que time_unit
        }

    * Paramètres optionnels (avec leur valeur par défaut)
        sep (\ *str*\ ) : Séparateur des fichiers de données, le même séparateur pour toutes les tables
        format_timestamp_target (\ *Date ou Timestamp*\ ) : Le format python sous lequel sont fournis les timestamps
        format_timestamp_log (\ *Date ou Timestamp*\ ) : Le format python sous lequel sont fournis les timestamps (le même pour toutes les tables)
        nb_targets (\ *int*\ ) : durée de la période de cibles, à exprimer dans la même unité que frequence_obs_cible


"""
