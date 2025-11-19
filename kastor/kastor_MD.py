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



"""
import pandas as pd
from datetime import timedelta
from os import path
from os import makedirs
from khiops import core as kh
from sys import exit
from time import time
from math import floor

from kastor._util import (
    convert_to_seconds,
    creation_list_datamarts_datetime,
    create_map_tables,
    create_map_entities_datetime,
    compare_data_tables_with_dictionary,
    compare_dictionary_with_data_tables,
    detect_format_timestamp,
    exist,
    exist_datamart,
    exist_in_dico_khiops,
    parse_name_file,
    work_path,
)


class KhiopsClassifier:
    """Classe pour effectuer la modélisation et le déploiement.


    """

    def __init__(
        self,
        dictionary,
        target_parameters,
        temporal_parameters,
        sep="\t",
        mobile=True,
        path_suffix = "",
        result_suffix = "",
        max_constructed_variables = 0,
        max_trees = 10,
    ):
        self.dictionary = dictionary
        self.target_parameters = target_parameters
        self.temporal_parameters = temporal_parameters
        self.sep = sep
        self.mobile = mobile
        self.path_suffix = path_suffix
        self.result_suffix = result_suffix
        self.max_constructed_variables = max_constructed_variables
        self.max_trees = max_trees


    def _modif_selection_dico_khiops_for_logs(
        self,
        dico_domain,
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

                    Unused Table(logs) logs;

                    Table(logs)	logsSelection =
                        TableSelection(logs, And(
                            LE(delta_units, Sum(7, 0, 60)),
                            GE(delta_units, Sum(7, 0))));
                        
                Table secondaire :

                    Unused Entity(sampledatamart) principal [line_id_sha];

                    Unused Numerical delta_units =
                        DiffDate(AsDate("2019-09-30","YYYY-MM-DD"),
                        GetDate(my_timestamp));


            Période mobile :

                Table principale :

                    Unused Table(logs) logs;

                    Table(logs)	logsSelection =
                        TableSelection(logs, And(
                            LE(delta_units, Sum(7, delta_target, 60)),
                            GE(delta_units, Sum(7, delta_target))));
                        
                Table secondaire :

                    Unused Entity(sampledatamart) principal [line_id_sha];

                    Unused Numerical delta_units =
                        DiffDate(GetValueD(principal, date_souscription),
                        GetDate(my_timestamp));

                    Unused Numerical delta_target =
                        GetValue(principal, delta_target_random);

        """
        # recuperation des path des tables dans data_tables
        # ce ne sera pas le même ordre que le dico khiops
        map_tables = create_map_tables(self.data_tables)

        # detection de format_timestamp_target
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

        # Dictionnaire root datamart : rajout des tables logs et des tables
        #                              TableSelection

        if not self.mobile:
            # date de fin de logs selon la date de début de cible
            # (jour, (ou heure, ou min) precedent)
            if period_unit == "days":
                date_end = start_date - timedelta(days=1)
                #date_start = date_end - timedelta(days=input_data_duration)
            elif period_unit == "hours":
                date_end = start_date - timedelta(hours=1)
                #date_start = date_end - timedelta(hours=input_data_duration)
            elif period_unit == "minutes":
                date_end = start_date - timedelta(minutes=1)
                #date_start = date_end - timedelta(minutes=input_data_duration)

        else:
            pass

        # conversion secondes pour Timestamp (multiplication par 1 pour Date)
        nb_seconds = convert_to_seconds(timestamp_target_type, period_unit)

        ## formule khiops recode string to Date or Timestamp  
        def recode_to_date(date):
            if timestamp_target_type == "Date":
                date_for_khiops = ('AsDate("'
                    + date.strftime(
                        format_timestamp_target_python
                    )
                    + '", "'
                    + format_timestamp_target_khiops
                    + '")'
                )
            elif timestamp_target_type == "Timestamp":
                date_for_khiops = ('AsTimestamp("'
                    + date.strftime(
                        format_timestamp_target_python
                    )
                    + '", "'
                    + format_timestamp_target_khiops
                    + '")'
                )
            return date_for_khiops
        
        # formule khiops difference Date or Timestamp  
        def difference_date_logs(date1, date2):
            if timestamp_target_type == "Date":
                diff_for_khiops = ('DiffDate(GetDate('
                    + date1
                    + '), '
                    + date2
                    + ')'
                )
            elif timestamp_target_type == "Timestamp":
                diff_for_khiops = ('DiffTimestamp('
                    + date1
                    + ', '
                    + date2
                    + ')'
                )
            return diff_for_khiops

        # pour calcul de delta_units	 
        if self.mobile:
            # GetValueD(principal, date_souscription)
            if timestamp_target_type == "Date":
                date_end_logs_str = (
                    "GetValueD(principal, " 
                    + name_var_date_target 
                    + ")"
                )  
            elif timestamp_target_type == "Timestamp":
                date_end_logs_str = (
                    "GetValueTS(principal, "
                    + name_var_date_target
                    + ")"
                )
        else:
            # AsDate("2019-09-30","YYYY-MM-DD")
            date_end_logs_str = recode_to_date(date_end)

        # nom de la table principale
        for dico in dico_domain.dictionaries:
            if dico.root:
                name_root = dico.name
                break
        
        # nom de la variable datetime de chaque table
        """
        {'LOGS': 'EVENT_DATE',
         'LOGS2': 'EVENT_DATE',
         ...}
        """        
        map_tables_timestamp = {}
        map_tables_no_timestamp = []
        for dico in dico_domain.dictionaries:
            if not dico.root:
                name_table_logs = dico.name
                # recherche de la table dans les noms de tables déclarées
                for key in map_tables.keys():
                    # pour chaque nom de table dans Khiops on cherche la table
                    # qui correspond dans data_tables pour récupérer le path
                    if key == name_table_logs:
                        my_timestamp_flag = False
                        try:
                            my_timestamp = self.data_tables["tables"][key][
                                "datetime"
                            ]
                        except KeyError:
                            map_tables_no_timestamp.append(name_table_logs)
                            
                        else :
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

        for dico in dico_domain.dictionaries:
            if dico.root:                     
                # Unused    Date    date_target        ;
                for var in dico.variables:
                    if var.name == name_var_date_target:
                        var.used = False
                        break

                if self.mobile:
                    # Unused    Numerical    delta_target_random        ;
                    var_delta = kh.Variable()

                    var_delta.name = "delta_target_random"
                    var_delta.type = "Numerical"
                    var_delta.used = False
                    dico.add_variable(var_delta)

                # rajout des tables logs sans datetime
                for name_table_logs in map_tables_no_timestamp:
                    # verifier si la table existe déjà
                    table = False
                    for var in dico.variables:
                        if var.name == name_table_logs:
                            var.used = False
                            table = True
                            break
                    if not table:
                        #     Table(logs)    logs        ;
                        var_logs = kh.Variable()
                        var_logs.name = name_table_logs
                        var_logs.type = "Table(" + name_table_logs + ")"
                        var_logs.used = True
                        dico.add_variable(var_logs)

                # rajout des tables logs et des tables TableSelection
                # avec datetime
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
                        var_logs = kh.Variable()
                        var_logs.name = name_table_logs
                        var_logs.type = "Table(" + name_table_logs + ")"
                        var_logs.used = False
                        dico.add_variable(var_logs)

                    # 	Table(logs)	logsSelection =
                    #       TableSelection(logs, And(
                    #       LE(delta_units, Sum(7, delta_target, 60)),        
                    #       GE(delta_units, Sum(7, delta_target))));
                    var_logs_selection = kh.Variable()
                    var_logs_selection.name = name_table_logs + "Selection"
                    var_logs_selection.type = "Table(" + name_table_logs + ")"
                    var_logs_selection.used = True
                    dico.add_variable(var_logs_selection)
                         
                    """
                    # var_logs_selection.rule = (
                    TableSelection(LOGS, 
                        And( 
                            LE(
                                delta_units,
                                Product(
                                    Sum(7, delta_target, 60),
                                    86400
                                    )
                                ),
                            GE(
                                delta_units, 
                                Product(
                                    Sum(7, delta_target), 
                                    86400
                                    )
                                )
                            )
                        )
                    )
                    """
                    if self.mobile:
                        delta_target_str = "delta_target"
                    else:
                        delta_target_str = "0"
                        
                    var_logs_selection.rule = (
                       "TableSelection(" + name_table_logs 
                       + ", And( LE(delta_units, Product(Sum("
                       + str(model_gap)
                       + ", " + delta_target_str + ", "
                       + str(input_data_duration)
                       + "), " + str(nb_seconds) + ")), "
                       "GE(delta_units, Product(Sum("
                       + str(model_gap)
                       + ", " + delta_target_str + "), " 
                       + str(nb_seconds) + "))))"
                       )

            else:  # elif dico_root == False:
                # pour chaque nom de table dans Khiops,
                # on cherche la table qui correspond dans data_tables
                name_table = dico.name
                # recherche de la table dans les noms de tables déclarées
                # avec datetime
                for name_table_logs in map_tables_timestamp.keys():
                    if name_table == name_table_logs:

                        name_var_id_table = self.data_tables["tables"][
                            name_table]["key"]
                        # Unused    Entity(sampledatamart)    principal[line_id_sha]    ;
                        var_entity_root = kh.Variable()

                        var_entity_root.name = "principal"
                        var_entity_root.rule = (
                            "[" + name_var_id_table + "]"
                        )
                        var_entity_root.type = "Entity"
                        var_entity_root.object_type = name_root
                        var_entity_root.used = False
                        dico.add_variable(var_entity_root)
                        
                        if self.mobile:
                            # Unused Numerical delta_target =
                            #   GetValue(principal, delta_target_random);
                            var_delta2 = kh.Variable()
    
                            var_delta2.name = "delta_target"
                            var_delta2.type = "Numerical"
                            var_delta2.rule = (
                                "GetValue(principal, delta_target_random)"
                            )
                            var_delta2.used = False
                            dico.add_variable(var_delta2)

                        # mobile
                        # Unused Numerical delta_units =
                        #   DiffDate(GetValueD(principal, date_souscription),
                        #           GetDate(my_timestamp));
                        
                        # fixe
                        # Unused Numerical delta_units =
                        #    DiffDate(AsDate("2019-09-30","YYYY-MM-DD"),
                        #    GetDate(my_timestamp));
                            
                        var_delta3 = kh.Variable()

                        var_delta3.name = "delta_units"
                        var_delta3.type = "Numerical"
                        if timestamp_target_type == "Date":
                            var_delta3.rule = (
                                "DiffDate("
                                + date_end_logs_str
                                + ", GetDate("
                                + map_tables_timestamp[dico.name]
                                + "))"
                            )
                        elif timestamp_target_type == "Timestamp":
                            # resultat de DiffTimestamp en secondes
                            var_delta3.rule = (
                                "DiffTimestamp("
                                + date_end_logs_str
                                + ", "
                                + map_tables_timestamp[dico.name]
                                + ")"
                            )
                        var_delta3.used = False
                        dico.add_variable(var_delta3)

                        break

        return name_root, dico_domain

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
            dico_domain = kh.read_dictionary_file(self.dictionary)

            for dico in dico_domain.dictionaries:
                if dico.root:
                    name_root = dico.name

                    # Numerical delta_target_random = Floor(Product(Random(), l));
                    var_delta = kh.Variable()

                    var_delta.name = "delta_target_random"
                    var_delta.type = "Numerical"
                    var_delta.rule = (
                        "Floor(Product(Random(), "
                        + str(target_duration)
                        + "))"
                    )
                    var_delta.used = True
                    dico.add_variable(var_delta)
            
            kh.deploy_model(
                dico_domain,  # dictionary file path or domain
                name_root,  # Name of the dictionary to deploy
                file,
                path.join(
                    file_sans_ext
                    + "_target"
                    + str(target_duration)
                    + extension
                ),
                field_separator=self.sep,
                output_field_separator=self.sep,
            )

    def _modif_selection_dico_khiops_for_datamart(
        self, 
        dico_domain,
        format_timestamp_target_khiops,
        format_timestamp_target_python,
        timestamp_target_type,
        period_unit,
    ):
        """
        Modification du dictionnaire à la volée pour la sélection
        du datamart le plus proche de la date d'événement
        
        * a compléter ou corriger *
        
            Période fixe

                Table principale :
                    
                    Unused	Table(DataMart1)	DataMart10		;	
                    Unused	Table(DataMart1)	DataMart11		;	
                    Unused	Table(DataMart1)	DataMart12		;	
                    Unused	Table(DataMart1)	DataMart10Selection	 = 
                    TableSelection(DataMart10, 
                                   GE( DiffDate(AsDate("01/09/2019", "DD/MM/YYYY"), 
                                                AsDate("01/09/2019", "DD/MM/YYYY")), 
                                      0))	;	
                    Unused	Table(DataMart1)	DataMart11Selection	 = 
                    TableSelection(DataMart11, 
                                   GE( DiffDate(AsDate("01/09/2019", "DD/MM/YYYY"), 
                                                AsDate("01/10/2019", "DD/MM/YYYY")), 
                                      0))	;	
                    Unused	Table(DataMart1)	DataMart12Selection	 = 
                    TableSelection(DataMart12, 
                                   GE( DiffDate(AsDate("01/09/2019", "DD/MM/YYYY"), 
                                                AsDate("01/11/2019", "DD/MM/YYYY")), 
                                      0))	;	
                    Unused	Table(DataMart1)	DataMart1Union	 = 
                    TableUnion(
                        DataMart12Selection,
                        DataMart11Selection,
                        DataMart10Selection
                        )	;	
                    Entity(DataMart1)	DataMart1	 = TableAt(DataMart1Union, 1)	;
                    

            Période mobile :

                Table secondaire :
                    
                    Unused	Table(DataMart1)	DataMart10		;	
                    Unused	Table(DataMart1)	DataMart11		;	
                    Unused	Table(DataMart1)	DataMart12		;	
                    Unused	Table(DataMart1)	DataMart10Selection	 = 
                    TableSelection(DataMart10, 
                                   GE( Diff(DiffDate(AsDate("08/10/2019", "DD/MM/YYYY"), 
                                                     AsDate("01/09/2019", "DD/MM/YYYY")), 
                                            Sum(7, 0)), 
                                      0))	;	
                    Unused	Table(DataMart1)	DataMart11Selection	 = 
                    TableSelection(DataMart11, 
                                   GE( Diff(DiffDate(AsDate("08/10/2019", "DD/MM/YYYY"), 
                                                     AsDate("01/10/2019", "DD/MM/YYYY")), 
                                            Sum(7, 0)), 
                                      0))	;	
                    Unused	Table(DataMart1)	DataMart12Selection	 = 
                    TableSelection(DataMart12, 
                                   GE( Diff(DiffDate(AsDate("08/10/2019", "DD/MM/YYYY"), 
                                                     AsDate("01/11/2019", "DD/MM/YYYY")), 
                                            Sum(7, 0)), 
                                      0))	;	
                    Unused	Table(DataMart1)	DataMart1Union	 = 
                    TableUnion(
                        DataMart12Selection,
                        DataMart11Selection,
                        DataMart10Selection
                        )	;	
                    Entity(DataMart1)	DataMart1	 = TableAt(DataMart1Union, 1)	;
                    
        """

        for dico in dico_domain.dictionaries:
            if dico.root:
                for key in self.data_tables["entities"].keys():

                    # lignes à rajouter
                    len_datamart = len(self.data_tables["entities"][key])
                    for i in range(len_datamart):
                        # 	Unused	Table(key) key0;
                        # 	Unused	Table(key) key1;
                        # 	Unused	Table(key) key2;
                        var_table = kh.Variable()
                        var_table.name = key + str(i)
                        var_table.type = "Table(" + key + ")"
                        var_table.used = False
                        dico.add_variable(var_table)


                    # Unused	Table(key)	key0Selection =
                    #   TableSelection(key0,
                    #       GE( Diff(DiffDate(AsDate("2019-09-15", "YYYY-MM-DD"),
                    #       AsDate("2019-09-01", "YYYY-MM-DD")),
                    #       Sum(7, .delta_target_random)), 0))		;
                    #
                    # création des variables seulement
                    # var_table.rule sera défini dans :
                    # _modif_selection_dico_khiops_datetime_for_datamart()
                    for i in range(len_datamart):
                        var_table = kh.Variable()
                        var_table.name = key + str(i) + "Selection"
                        var_table.type = "Table(" + key + ")"
                        var_table.used = False
                        dico.add_variable(var_table)

                    # Unused	Table(key)	keyUnion	 =
                    #   TableUnion(
                    #       key2Selection,
                    #       key1Selection,
                    #       key0Selection
                    #   )	;

                    var_table = kh.Variable()
                    var_table.name = key + "Union"
                    var_table.type = "Table(" + key + ")"
                    rule = "TableUnion("
                    for i in reversed(range(len_datamart)):
                        rule += key + str(i) + "Selection,"
                    rule = rule[:-1] + ")"
                    var_table.rule = rule
                    var_table.used = False
                    dico.add_variable(var_table)
                    
                    # Entity(key) key = TableAt(keyUnion, 1)	;
                    # verifier si l'entity existe déjà
                    entity = False
                    for var in dico.variables:
                        if var.name == key:
                            entity = True
                            var.rule = "TableAt(" + key + "Union, 1)"
                            break
                    if not entity:
                        var_entities = kh.Variable()
                        var_entities.name = key
                        var_entities.type = "Entity(" + key + ")"
                        var_entities.rule =   "TableAt(" + key + "Union, 1)"                      
                        dico.add_variable(var_entities)                    
                    
                break

        return dico_domain
    
    def _modif_selection_dico_khiops_datetime_for_datamart(
        self,
        dico_domain,
        my_date,
        format_timestamp_target_khiops,
        format_timestamp_target_python,
        timestamp_target_type,
        name_var_date_target,
        period_unit,
        model_gap,
    ):
        """
        Modification du dictionnaire à la volée pour fit et pour predict

        ..note:: en modélisation :

            Dans la table root pour chacun des datamarts selection des datetime
            # Unused	Table(SNB_name_of_the_first_entity_table)
            #               name_of_the_first_entity_table0Selection =
            #               TableSelection(name_of_the_first_entity_table0,
            #               GE( Diff(DiffDate(.EVENT_DATE,
            #               AsDate("2019-09-01", "YYYY-MM-DD")),
            #               Sum(7, .delta_target_random)), 0))		;

        ..note:: en déploiement la date est modifiée à chaque pas :

            Dans la table root pour chacun des datamarts selection des datetime
            # Unused	Table(SNB_name_of_the_first_entity_table)
            #               name_of_the_first_entity_table0Selection =
            #               TableSelection(name_of_the_first_entity_table0,
            #               GE( Diff(DiffDate(AsDate(my_date, "YYYY-MM-DD"),
            #               AsDate("2019-09-01", "YYYY-MM-DD")),
            #               Sum(7, .delta_target_random)), 0))		;
        """
        
        # définition de str_delta_target_random :
        # en fit :
        #   fixe : str_delta_target_random = "0"
        #   mobile : str_delta_target_random = ".delta_target_random"
        # en predict :
        #   str_delta_target_random = "0"
        if self.mobile:
            str_delta_target_random = ".delta_target_random"
        else:
            str_delta_target_random = "0"
        
        # formule khiops recode string to Date or Timestamp    
        def recode_to_date(date):
            if timestamp_target_type == "Date":
                date_for_khiops = ('AsDate("'
                    + date.strftime(
                        format_timestamp_target_python
                    )
                    + '", "'
                    + format_timestamp_target_khiops
                    + '")'
                )
            elif timestamp_target_type == "Timestamp":
                date_for_khiops = ('AsTimestamp("'
                    + date.strftime(
                        format_timestamp_target_python
                    )
                    + '", "'
                    + format_timestamp_target_khiops
                    + '")'
                )
            return date_for_khiops

        # formule khiops difference Date or Timestamp    
        def difference_date_datamart(date1, date2):
            if timestamp_target_type == "Date":
                diff_for_khiops = ('DiffDate('
                    + date1
                    + ', '
                    + date2
                    + ')'
                )
            elif timestamp_target_type == "Timestamp":
                diff_for_khiops = ('DiffTimestamp('
                    + date1
                    + ', '
                    + date2
                    + ')'
                )
            return diff_for_khiops
        
        # fit
        if my_date == "":
            # .EVENT_DATE
            my_date_for_khiops = "." + name_var_date_target
            
        # predict
        else :
            # AsDate(my_date, "YYYY-MM-DD")
            my_date_for_khiops = recode_to_date(my_date)
            
        # conversion secondes pour Timestamp (multiplication par 1 pour Date)
        nb_seconds = convert_to_seconds(timestamp_target_type, period_unit)

        modif = False
        for dico in dico_domain.dictionaries:
            if dico.root:
                # Rajout des variables dans toutes les entities
                for key in self.data_tables["entities"].keys():
                    len_datamart = len(self.data_tables["entities"][key])
                    for i in range(len_datamart):
                        # Unused	Table(SNB_name_of_the_first_entity_table)
                        #           name_of_the_first_entity_table0Selection =
                        #           TableSelection(name_of_the_first_entity_table0,
                        #           GE(Diff(DiffDate(AsDate(my_date, "YYYY-MM-DD"),
                        #           AsDate("2019-09-01", "YYYY-MM-DD")),
                        #           Sum(7, .delta_target_random)), 0))		;
                        for var in dico.variables:
                            if var.name == (key + str(i) + "Selection"):
                                datetime_ref = self.data_tables["entities"][key][i][
                                        "datetime"
                                    ]
                                var.rule = (
                                    "TableSelection("
                                    + key + str(i)
                                    + ", GE( Diff("
                                    + difference_date_datamart(
                                        my_date_for_khiops, 
                                        recode_to_date(datetime_ref)
                                        )
                                    + ", Product(Sum("
                                    + str(model_gap)
                                    + ", " + str_delta_target_random + "), "
                                    + nb_seconds + ")), 0))"  
                                )
                                modif = True

        if not modif:
            print(
                "Attention la date de déploiement n'est pas prise en compte "
                + "pour la sélection des datamarts, "
                + "vérifiez les données"
            )
            exit()

        return dico_domain

    def fit(self, data_tables_train):
        """
        Modélisation sur période fixe et/ou sur période mobile
        """
        debut = time()
        self.data_tables = data_tables_train
        # vérification de la cohérence des tables déclarées entre data_tables 
        # et le dictionnaire khiops
        compare_data_tables_with_dictionary(
            self.dictionary, self.data_tables, self.target_parameters
        )
        compare_dictionary_with_data_tables(
            self.dictionary, self.data_tables, self.target_parameters
        )
        
        name_var_date_target = self.target_parameters["datetime"]
        target = self.target_parameters["target"]
        period_unit = self.temporal_parameters["period_unit"]
        input_data_duration = self.temporal_parameters["input_data_duration"]
        start_date = self.temporal_parameters["start_date"]
        model_gap = self.temporal_parameters["model_gap"]

        # detection de format_timestamp_target
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

        # vérification de l'existence du fichier train
        file_train = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_train)
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

        print("dictionnaire :" + self.dictionary)
        dico_domain = kh.read_dictionary_file(self.dictionary)
        exist_in_dico_khiops(dico_domain, target)
        exist_in_dico_khiops(dico_domain, name_var_date_target)
        
        rep_result = work_path(rep, self.mobile, self.path_suffix)

        # modification du dictionnaire à la volee pour sélection des logs - sc2
        (
            name_root,
            dico_domain,
        ) = self._modif_selection_dico_khiops_for_logs(
            dico_domain,
            name_var_date_target,
            start_date,
            period_unit,
            model_gap,
            input_data_duration,
        )
             
        # vérification de l'existence de datamarts
        is_datamart = exist_datamart(self.data_tables)
        # lecture des tables secondaires
        if is_datamart:
            # on a besoin de tous les datamarts
            if not self.mobile:
                model_gap = 0
            
            # modification du dico khiops pour renseigner les datamarts temporels
            # et sélectionner le plus proche
            dico_domain = self._modif_selection_dico_khiops_for_datamart(
                dico_domain,
                format_timestamp_target_khiops,
                format_timestamp_target_python,
                timestamp_target_type,
                period_unit,
            )

            # pour fit : nom de la variable datetime EVENT_DATE
            # pour predict : jour de deploiement
            dico_domain = self._modif_selection_dico_khiops_datetime_for_datamart(
                dico_domain,
                "",
                format_timestamp_target_khiops,
                format_timestamp_target_python,
                timestamp_target_type,
                name_var_date_target,
                period_unit,
                model_gap,
            )
 
        additional_table_modeling = (
            self._lecture_additional_data_tables(dico_domain, is_datamart)
        )            
 
        # ecriture du dico khiops
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
        
        # si paramètres par défaut modifiés
        if self.max_constructed_variables == 0 :
            max_constructed_variables=(1000 * input_data_duration)
        else:
            max_constructed_variables = self.max_constructed_variables
        
        # modelisation
        kh.train_predictor(
            dico_domain,  # dictionary file path or domain
            name_root,  # name of the table's dictionary
            file_fit,  # data table file path
            target,  # target variable name
            rep_result,
            sample_percentage=100,
            field_separator = self.sep,
            max_trees = self.max_trees,
            additional_data_tables = additional_table_modeling,
            max_constructed_variables = max_constructed_variables,
        )

        duree = time()-debut
        print("durée d'exécution : " +str(timedelta(seconds=duree)))

    def _lecture_additional_data_tables(self, dico_domain, is_datamart):
        """
        Construction de *additional_table_modeling* à partir de
        *map_entities_datetime* et *map_tables*

        {'TablePrincipale`DataMart10': '...\\sample2_synthetic_DM1_07.csv', 
         'TablePrincipale`DataMart11': '...\\sample2_synthetic_DM1_08.csv', 
         'TablePrincipale`DataMart12': '...\\sample2_synthetic_DM1_09.csv',
         'TablePrincipale`LOGS': '...\\sample2_synthetic_log.csv'
         }
        """

        additional_table_modeling = {}

        # recuperation des path des tables et entities dans data_tables
        # ce ne sera pas le même ordre que le dico khiops


        for dico in dico_domain.dictionaries:
            if dico.root:
                name_root = dico.name
                break

        # datamarts
        if is_datamart:
            map_entities_datetime = create_map_entities_datetime(self.data_tables)
            for key, datetime_str, key_number in map_entities_datetime:
                key_flag = False
                # pour chaque nom d'entity dans data_tables on cherche
                # le dico qui correspond dans Khiops pour récupérer le path
                for dico in dico_domain.dictionaries:
                    if not dico.root:
                        # suppression du prefixe 'SNB_' pour rechercher le nom dans
                        # map_tables_entities
                        name_dico = dico.name
                        name_dico = name_dico.replace("SNB_","")
                        if name_dico == key:
                            key_flag = True
                            # additional_table_modeling['TablePrincipale`DataMart10']
                            #     =  '...\\sample2_synthetic_DM1_07.csv'
                            additional_table_modeling[
                                name_root + "`" + key_number
                            ] = map_entities_datetime[(key, datetime_str, key_number)]
                            break
                if not key_flag:
                    print(
                        "Le nom de la table '"
                        + key
                        + "' déclarée dans data_tables "
                        + " n'existe pas dans le dictionnaire khiops "
                    )
                    exit()             

        # tables
        map_tables = create_map_tables(self.data_tables)

        for key in map_tables.keys():
            key_flag = False
            # pour chaque nom de table dans data_tables on cherche
            # le dico qui correspond dans Khiops pour récupérer le path
            for dico in dico_domain.dictionaries:
                if not dico.root:
                    # suppression du prefixe 'SNB_' pour rechercher le nom dans
                    # map_tables_entities
                    name_dico = dico.name
                    name_dico = name_dico.replace("SNB_","")
                    if name_dico == key:
                        key_flag = True
                        # additional_table_modeling['TablePrincipale`LOGS']
                        #     =  '...\\sample2_synthetic_log.csv'
                        additional_table_modeling[
                            name_root + "`" + key
                        ] = map_tables[key]
                        break
            if not key_flag:
                print(
                    "Le nom de la table '"
                    + key
                    + "' déclarée dans data_tables "
                    + " n'existe pas dans le dictionnaire khiops "
                )
                exit() 
        
        return additional_table_modeling
  
    def _modif_selection_dico_khiops_datetime_for_logs(
        self, 
        dico_domain, 
        my_date, 
        format_timestamp_target_khiops,
        format_timestamp_target_python, 
        timestamp_target_type,
    ):
        """
        Modification du dictionnaire à la volée pour le déploiement

        .. note:: La date de déploiement est modifiée à chaque pas :
            Dans chacune des tables
            # Unused    Numerical    delta_units     =
            #   DiffDate(AsDate("2020-09-01", "YYYY-MM-DD"),
            #           GetDate(my_timestamp))    ;
        """
        modif = False
        for dico in dico_domain.dictionaries:
            if not dico.root:
                name_table_logs = dico.name
                for key in self.data_tables["tables"].keys():
                    keySNB = "SNB_" + key
                    if keySNB == name_table_logs:
                        # récupération du nom des variables Timestamp
                        try:
                            my_timestamp = self.data_tables["tables"][key][
                                "datetime"]
                        except KeyError:
                            pass
                        else:
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
    
                            # Unused    Numerical    delta_units     =
                            #           DiffDate(AsDate("2020-09-01", "YYYY-MM-DD"),
                            #           GetDate(my_timestamp))    ;
                            for var in dico.variables:
                                if var.name == "delta_units":
                                    if timestamp_target_type == "Date":
                                        var.rule = (
                                            'DiffDate(AsDate("'
                                            + my_date.strftime(
                                                format_timestamp_target_python
                                            )
                                            + '", "'
                                            + format_timestamp_target_khiops
                                            + '"), GetDate('
                                            + my_timestamp
                                            + "))"
                                        )
                                        modif = True
                                    elif (
                                        timestamp_target_type == "Timestamp"
                                    ):
                                        var.rule = (
                                            'DiffTimestamp(AsTimestamp("'
                                            + my_date.strftime(
                                                format_timestamp_target_python
                                            )
                                            + '", "'
                                            + format_timestamp_target_khiops
                                            + '"), '
                                            + my_timestamp
                                            + ")"
                                        )
                                        modif = True
                                    break
        if not modif:
            print(
                "attention la date de déploiement n'est pas prise en compte "
                + "pour la sélection des logs, "
                + "vérifiez les données"
            )
            exit()

        return dico_domain
    def _modif_selection_dico_khiops_delta_target_for_logs_for_depl_mobile(
        self,
        dico_domain,
    ):   
        # modification du dictionnaire Modeling.kdic
        # suite à la modification "pas de delta_target en déploiement"
        # dans Logs suppression de delta_target
        # dans Root suppression de delta_target_random
        #remplacer delta_target par 0 dans les formules

        for dico in dico_domain.dictionaries:
            if dico.root:
                snb_root_dictionary = dico
            else:
                for var in dico.variables:
                    if var.name == 'delta_target':
                        dico.remove_variable(var.name)
                      
        for var in snb_root_dictionary.variables:
            if var.name == 'delta_target_random':
                snb_root_dictionary.remove_variable(var.name)
            else:
                var.rule = var.rule.replace('.delta_target_random', '0')
                var.rule = var.rule.replace('delta_target', '0')
        return dico_domain
    

    def predict(self, data_tables_test):
        """
        Le déploiement est effectué à partir de la date spécifiée 
        (ou de la date de début) ,
        par pas de *period_unit*, autant de fois que *period_nb*

        Déploiement sur période fixe et/ou sur période mobile :
            | Sur une période fixe définie (par exemple 1 mois en marketing)
            | Sur une période mobile : les traitements sont effectués sur une période
                mobile définie en paramètre (par exemple 1, 7 ou 15 jours en marketing)


        """
        debut = time()
        self.data_tables = data_tables_test
        """ A VOIR : on ne fait pas le contrôle à cause du déploiement :
        le controle est fait en fit et on suppose que c'est ok ensuite ?
        """
        # vérification de la cohérence des tables déclarées entre data_tables 
        # et le dictionnaire khiops
        compare_data_tables_with_dictionary(
            self.dictionary, self.data_tables, self.target_parameters
        )
        compare_dictionary_with_data_tables(
            self.dictionary, self.data_tables, self.target_parameters
        )
        
        name_var_id = self.data_tables["main_table"]["key"]
        name_var_date_target = self.target_parameters["datetime"]
        target = self.target_parameters["target"]
        main_target_modality = self.target_parameters["main_target_modality"]

        period_unit = self.temporal_parameters["period_unit"]
        model_gap = self.temporal_parameters["model_gap"]
        
        if self.mobile:
            target_duration = self.temporal_parameters["target_duration"]
        else :
            target_duration = 1
        
        try:
            start_date = self.temporal_parameters["start_date"]
        except KeyError:
            print(
                "Erreur -> pour le déploiement "
                + "temporal_parameters[\"start_date\"] doit être renseignée"
            )
            exit()
        
        # recuperation ou calcul de period_nb
        end_date_flag = False    
        try:
            period_nb = self.temporal_parameters["period_nb"]
        except KeyError:
            try:
                end_date = pd.Timestamp(self.temporal_parameters["end_date"])
                end_date_flag = True
            except KeyError:
                period_nb = 1

        if end_date_flag:
            diff = end_date - start_date
            if period_unit == "days":
                period_nb = diff.days
                    
            elif period_unit == "hours":
                period_nb = floor(diff.total_seconds() / 3600)
                    
            elif period_unit == "minutes":
                period_nb = floor(diff.total_seconds() / 60)
                
        # pour suffixer les noms des sorties suivantes :
        # TransferDatabase, score_by_period_unit, target_by_period_unit, 
        # eval_reactif, precison, rappel
        if self.result_suffix != "":
            result_suffix = "_" + self.result_suffix
        
        # vérification de l'existence du fichier test
        file_test = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_test)
        exist(file_test)

        print("fichier test : " + file_test)
        df_test = pd.read_csv(file_test, sep=self.sep)
        rep_result = work_path(rep, self.mobile, self.path_suffix)

        transfer_directory = path.join(
            rep_result,
            "TransferDatabase" + result_suffix
        )
        if not path.exists(transfer_directory):
            makedirs(transfer_directory)

        # extraction du nom du fichier et de l extension
        file_test_without_ext, extension = parse_name_file(file_test)

        # vérification de l'existence de datamarts
        is_datamart = exist_datamart(self.data_tables)

        # detection de format_timestamp_target
        name_dico_main = (
            'SNB_' + self.data_tables["main_table"]["name_main_table"]
            )
        dico_ref = path.join(rep_result, "Modeling.kdic")
        (
            format_timestamp_target_khiops,
            format_timestamp_target_python,
            timestamp_target_type,
        ) = detect_format_timestamp(
            dico_ref,
            name_dico_main,
            name_var_date_target,
        )
        
        # modification du dictionnaire Modeling.kdic
        # on garde ID et ProbTARGET1, les autres variables en Unused

        dico_domain = kh.read_dictionary_file(dico_ref)
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
            
        # Déploiement sur period_nb
        """
        le modèle est déployé de start_date à start_date + period_nb
        déploiement à la date start_date -> transfer_1
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
                self.data_tables
            )

        # initialisation date de début de déploiement
        depl_date = start_date
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
            
        additional_table_modeling = (
            self._lecture_additional_data_tables(dico_domain, is_datamart)
        )   
        #dico_domain.export_khiops_dictionary_file(path.join(rep_result,
        #'dico_init.kdic'))

        # fixe
        if not self.mobile:
            for step in range(period_nb):
                # pour chaque pas on regarde si cela crée un nouvel élément
                # dans la liste
                datamart_datetime_depl = ""
                name_depl = "transfer"
                if is_datamart:
                    for datamart_datetime in list_datamarts_datetime:
                        if (depl_date - gap) >= datamart_datetime:
                            datamart_datetime_depl = datamart_datetime
                            name_depl = "transfer_" + str(datamart_datetime_depl)
                    if datamart_datetime_depl == "":
                        print(
                            "les datetime des tables entities doivent couvrir "
                            + "les dates de déploiements, or la date '"
                            + str(depl_date)
                            + "' n'est pas couverte"
                        )
                        exit()
                #print(str(depl_date) + ' -> ' + name_depl)
                
                if name_depl not in list_depl:
                    list_depl.append(name_depl)
                    num_depl = len(list_depl)

                    # on regarde si c'est un nouvel élément dans la liste
                    # -> si oui un nouveau déploiement
                    if num_depl > len_list_depl:
                        # mise à jour de la taille de la liste
                        len_list_depl = num_depl

                        # on prend les logs jusqu'à J-1 (ou H-1 ou min-1)
                        if period_unit == "days":
                            logs_datetime_depl = depl_date - timedelta(days=1)
                        elif period_unit == "hours":
                            logs_datetime_depl = depl_date - timedelta(hours=1)
                        elif period_unit == "minutes":
                            logs_datetime_depl = depl_date - timedelta(minutes=1)
                        
                        # modification du dictionnaire Modeling.kdic
                        # saisie de la date de depl pour la sélection des logs 
                        
                        dico_domain = (
                            self._modif_selection_dico_khiops_datetime_for_logs(
                            dico_domain, 
                            logs_datetime_depl, 
                            format_timestamp_target_khiops,
                            format_timestamp_target_python, 
                            timestamp_target_type,
                            )
                        )
                        
                        if is_datamart:
                            # saisie de la date de depl pour la sélection des datamarts
                            dico_domain = (
                                self._modif_selection_dico_khiops_datetime_for_datamart(
                                dico_domain,
                                datamart_datetime_depl,
                                format_timestamp_target_khiops,
                                format_timestamp_target_python,
                                timestamp_target_type,
                                name_var_date_target,
                                period_unit,
                                model_gap,
                                )
                            )
                        dico_domain.export_khiops_dictionary_file(
                            path.join(transfer_directory, 
                                      'dico_' + str(num_depl) + '.kdic')
                            )
                        
                        # Transfert
                        
                        kh.deploy_model(
                            dico_domain,  # dictionary file path or domain
                            name_root,  # name of the modeling dictionary
                            file_test,  # data table file
                            path.join(
                                transfer_directory,
                                "transfer_" + str(num_depl) + ".csv",
                            ),  # output data table file
                            field_separator=self.sep,
                            additional_data_tables=additional_table_modeling,
                        )

                # on décale d'une unité period_unit
                if period_unit == "days":
                    depl_date = depl_date + timedelta(days=1)
                elif period_unit == "hours":
                    depl_date = depl_date + timedelta(hours=1)
                elif period_unit == "minutes":
                    depl_date = depl_date + timedelta(minutes=1)
            
            print("répertoire : " 
                  + transfer_directory
                  + " --> nombre de déploiements : " + str(len_list_depl))

        # mobile
        else:
            for step in range(period_nb):
                num_depl = step
                
                # modification du dictionnaire Modeling.kdic           
                # saisie de la date de depl pour la sélection des logs       
                dico_domain = (
                    self._modif_selection_dico_khiops_datetime_for_logs(
                    dico_domain, 
                    depl_date, 
                    format_timestamp_target_khiops,
                    format_timestamp_target_python, 
                    timestamp_target_type,
                    )
                )

                if is_datamart:
                    
                    # saisie de la date de depl pour la sélection des datamarts
                    dico_domain = (
                        self._modif_selection_dico_khiops_datetime_for_datamart(
                        dico_domain,
                        depl_date,
                        format_timestamp_target_khiops,
                        format_timestamp_target_python,
                        timestamp_target_type,
                        name_var_date_target,
                        period_unit,
                        model_gap,
                        )
                    )

                dico_domain = (
                    self._modif_selection_dico_khiops_delta_target_for_logs_for_depl_mobile(
                    dico_domain,
                    )
                )
                
                dico_domain.export_khiops_dictionary_file(path.join(
                    transfer_directory, 
                    'dico_' + str(num_depl) + '.kdic')
                    )
                
                # transfert
                
                kh.deploy_model(
                    dico_domain,  # dictionary file path or domain
                    name_root,  # name of the modeling dictionary
                    file_test,  # data table file
                    path.join(
                        transfer_directory,
                        "transfer_" + str(num_depl) + ".csv",
                    ),  # output data table file
                    field_separator=self.sep,
                    additional_data_tables=additional_table_modeling,
                )

                # on décale d'une unité period_unit
                if period_unit == "days":
                    depl_date = depl_date + timedelta(days=1)
                elif period_unit == "hours":
                    depl_date = depl_date + timedelta(hours=1)
                elif period_unit == "minutes":
                    depl_date = depl_date + timedelta(minutes=1)

            print("répertoire : " 
                  + path.join(transfer_directory)
                  + " --> nombre de déploiements : " + str(period_nb))

        # Constitution de la table des scores
        # récupération de la liste des ids
        df_liste_ids_test = df_test[name_var_id]
        
        # concatenation des fichiers transferts
        #df_score = df_target.loc[:,[name_var_id]]
        df_score = df_liste_ids_test
        
        nb_scores = period_nb

        df_score = self._create_score_by_period_unit_file(
            df_score,
            rep_result,
            name_var_id,
            target,
            main_target_modality,
            start_date,
            format_timestamp_target_python,
            period_unit,
            nb_scores,
            is_datamart,
        )
        df_score.to_csv(
            path.join(
                rep_result, 
                "score_by_period_unit" + result_suffix + ".csv"
            ),
            sep=self.sep,
            index=False
        )

        # constitution du fichier cible par period_unit
        # si la variable cible existe dans df_test
        try :
            df_test[target]

        except KeyError:
            duree = time()-debut
            print("durée d'exécution : " +str(timedelta(seconds=duree)))
            return df_score
        
        else:
            #nb_targets = period_nb
            #nb_targets = period_nb + target_duration
            nb_targets = period_nb + target_duration - 1  
           
            df_target = self._create_target_by_period_unit_file(
                name_var_id,
                file_test,
                target,
                main_target_modality,
                name_var_date_target,
                start_date,
                format_timestamp_target_python,
                period_unit,
                nb_targets,
            )
            df_target.to_csv(
                path.join(
                    rep_result, 
                    "target_by_period_unit" + result_suffix + ".csv"
                ),
                sep=self.sep,
                index=False
            )
            duree = time()-debut
            print("durée d'exécution : " +str(timedelta(seconds=duree)))
            return df_score, df_target 
        

    def _create_target_by_period_unit_file(
        self,
        name_var_id,
        name_file_test,
        target,
        main_target_modality,
        name_var_date_target,
        start_date,
        format_timestamp_target_python,
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
                df_target[name_var_date_target],
                format=format_timestamp_target_python,
            )
            # arrondi a l heure ou minute inferieure
            df_target[name_var_date_target] = df_target[
                name_var_date_target
            ].dt.floor(decoupage)
            # on repasse la date en objet pour calculer date * cible
            # df_target[name_var_date_target] = df_target[
            #                       name_var_date_target
            #                   ].astype(str)

        # creation de la liste des dates
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
            if str(row[target]) == str(main_target_modality):
                if str(row[name_var_date_target]) == str(
                    date.strftime(format_timestamp_target_python)
                ):
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

    def _create_score_by_period_unit_file(
        self,
        df_score,
        rep_result,
        name_var_id,
        target,
        main_target_modality,
        start_date,
        format_timestamp_target_python,
        period_unit,
        nb_scores,
        is_datamart,
    ):
        """Concaténation des 2 dataframes cibles et scores"""

        # creation de la liste des datetime disponibles dans la définition des
        # datamarts
        if is_datamart:
            list_datamarts_datetime = creation_list_datamarts_datetime(
                self.data_tables
            )

        # lecture des fichiers transfer
        my_date = start_date
        list_depl = []
        
        for step in range(nb_scores):
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
                            "les datetime des tables entities doivent couvrir "
                            + "les dates de déploiements, or la date '"
                            + str(my_date)
                            + "' n'est pas couverte"
                        )
                        exit()

                if name_depl not in list_depl:
                    list_depl.append(name_depl)

            # récupération du nom du fichier transfer
            if self.result_suffix != "":
                result_suffix = "_" + self.result_suffix
            if self.mobile:
                num_depl = step
            else:
                num_depl = len(list_depl)
            file_transfer = path.join(
                rep_result,
                "TransferDatabase" + result_suffix,
                "transfer_" + str(num_depl) + ".csv",
            )
            df = pd.read_csv(file_transfer, sep="\t")
            df = df[[name_var_id, "Prob" + target + str(main_target_modality)]]

            df.columns = [
                name_var_id,
                "score_" + my_date.strftime(format_timestamp_target_python),
            ]
            df_score = pd.merge(df_score, df, how="inner", on=name_var_id)
            #print("score_" + my_date.strftime(format_timestamp_target_python))

            if period_unit == "days":
                my_date += timedelta(days=1)
            elif period_unit == "hours":
                my_date += timedelta(hours=1)
            elif period_unit == "minutes":
                my_date += timedelta(minutes=1)

        return df_score



       
