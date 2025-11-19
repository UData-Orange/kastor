from math import *
from tqdm import tqdm
from khiops import core as kh
import re
import os
import pandas as pd
import sys
import json
from sys import exit

# Ajouter le chemin du dossier parent
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "")))

# Dictionary to match TablePartiton with construction rule
Dict_matching_partition = {
    "TablePartitionCount": "TableCount",
    "TablePartitionCountDistinct": "TableCountDistinct",
    "TablePartitionMode": "TableMode",
    "TablePartitionModeAt": "TableModeAt",
    "TablePartitionMean": "TableMean",
    "TablePartitionStdDev": "TableStdDev",
    "TablePartitionMedian": "TableMedian",
    "TablePartitionMin": "TableMin",
    "TablePartitionMax": "TableMax",
    "TablePartitionSum": "TableSum",
}

# Dictionary to match primitive name with construction rule
Dict_matching_rule_name = {
    "Count": "TableCount",
    "CountDistinct": "TableCountDistinct",
    "Mode": "TableMode",
    "ModeAt": "TableModeAt",
    "Mean": "TableMean",
    "StdDev": "TableStdDev",
    "Median": "TableMedian",
    "Min": "TableMin",
    "Max": "TableMax",
    "Sum": "TableSum",
    "Date": "GetDate",
    "Time": "GetTime",
}

def get_key(dictionary, val):
    """Get the key of a value in a python dictionary

    :param dictionary: Python dictionary.
    :type dictionary: dict
    :param val: Value to test.
    :return: The key associated to the value

    """
    for key, value in dictionary.items():
        if value == val:
            return key
    return -1

def add_noise(dictionary_domain, number_noise):
    """Add noise variable into secondaries tables.

    :param dictionary_domain: Khiops dictionary domain
    :param number_noise: Number of noise variable to add per table
    :type number_noise: int
    :return: Khiops dictionary domain with noise
    """
    # Initialize
    index = 0
    noise_variable = []
    for dictionary in dictionary_domain.dictionaries:
        if not dictionary.root:
            number_add_noise = 0
            for i in range(number_noise):
                # Add numerical noise variable
                if number_add_noise < number_noise:
                    variable = kh.Variable()
                    variable.name = "N_" + str(index)
                    noise_variable.append(variable.name)
                    variable.type = "Numerical"
                    variable.used = True
                    variable.rule = "Sum(Random()," + str(i) + ")"
                    dictionary_domain.get_dictionary(dictionary.name).add_variable(
                        variable
                    )
                    noise_variable.append(variable.name)
                    number_add_noise += 1
                # Add categorical noise variable
                if number_add_noise < number_noise:
                    variable = kh.Variable()
                    variable.name = "C_" + str(index)
                    noise_variable.append(variable.name)
                    variable.type = "Categorical"
                    variable.used = True
                    variable.rule = (
                        'Concat("V_",FormatNumerical(Round(Product('
                        + str(2 ** (i + 1))
                        + ",Random())),0,0))"
                    )
                    dictionary_domain.get_dictionary(dictionary.name).add_variable(
                        variable
                    )
                    noise_variable.append(variable.name)
                    number_add_noise += 1
                index += 1
                if number_add_noise >= number_noise:
                    break
    return dictionary_domain, noise_variable

def get_dataset(dataset_name):
    if dataset_name == "Accident_star":
        # Set accident data information
        commun_path = os.path.join("notebook", "data", "Accident")
        dictionary_file_path = os.path.join(commun_path, "Accidents_etoile.kdic")
        data_table_path = os.path.join(commun_path, "Accidents.txt")
        vehicle_table_path = os.path.join(commun_path, "Vehicles.txt")
        user_table_path = os.path.join(commun_path, "Users.txt")
        place_table_path = os.path.join(commun_path, "Places.txt")
        main_dictionary_name = "Accident"
        Additional_data_tables = {
            main_dictionary_name + "`Place": place_table_path,
            main_dictionary_name + "`Vehicles": vehicle_table_path,
            main_dictionary_name + "`Users": user_table_path,
        }
        target = "Gravity"
        return (
            commun_path,
            dictionary_file_path,
            data_table_path,
            [vehicle_table_path, user_table_path, place_table_path],
            Additional_data_tables,
            main_dictionary_name,
            target,
        )

    if dataset_name == "synth1":
        # Set data information
        commun_path = os.path.join("notebook", "data", "synth1")
        dictionary_file_path = os.path.join(commun_path, "sample2_synthetic_dm_ids.kdic")
        data_table_path = os.path.join(commun_path, "sample2_synthetic_ids.csv")
        datamart_path = os.path.join(commun_path, "sample2_synthetic_DM1_07.csv")
        logs_table_path = os.path.join(commun_path, "sample2_synthetic_log.csv")

        main_dictionary_name = "TablePrincipale"
        Additional_data_tables = {
            main_dictionary_name + "`DataMart1": datamart_path,
            main_dictionary_name + "`LOGS": logs_table_path,
        }
        target = "TARGET"
        return (
            commun_path,
            dictionary_file_path,
            data_table_path,
            [datamart_path, logs_table_path],
            Additional_data_tables,
            main_dictionary_name,
            target,
        )


class UnivariateMultitableAnalysis:
    """Estimates the importance of secondary native variables in multitable
    data on the target variable using a univariate approach with or without
    discretization.

    :param dictionary_file_path: Path of a Khiops dictionary file.
    :type dictionary_file_path: str
    :param dictionary_name: Name of the dictionary to be analyzed.
    :type dictionary_name: str
    :param data_table_path: Path of the data table file.
    :type data_table_path: str
    :param additional_data_tables: A dictionary containing the data paths and file paths for a multi-table dictionary file.
    :type additional_data_tables: dict
    :param target_variable: Name of the target variable.
    :type target_variable: str
    :param exploration_type: Parameter to be analyze, 'All' for both variable and primitive, 'Variable' or 'Primitive for only variable or primitive,  defaults to 'Variable'.
    :type exploration_type: str
    :param count_effect_reduction: State of discretisation, True is used, defaults to True.
    :type count_effect_reduction: bool
    :param max_trees: Maximum number of trees to construct, defaults to 0.
    :type max_trees: int, optional
    :param max_constructed_variables_per_variable: Maximum number of variables to construct per native variable, defaults to 10.
    :type max_constructed_variables_per_variable: int, optional
    :param results_dir: Path of the results directory, defaults to "Results".
    :type results_dir: str, optional
    :param construction_rules: Allowed rules for the automatic variable construction, defaults to kh.all_construction_rules.
    :type construction_rules: list, optional
    :param output_dir: Path of the output directory, defaults to "".
    :type output_dir: str, optional
    :param results_prefixe: Prefix of the result files, defaults to "".
    :type results_prefixe: str, optional
    """

    def __init__(
        self,
        dictionary_file_path,
        dictionary_name,
        data_table_path,
        additional_data_tables,
        target_variable,
        exploration_type="Variable",
        count_effect_reduction=True,
        max_trees=0,
        max_constructed_variables_per_variable=10,
        construction_rules=kh.all_construction_rules,
        results_dir="Results",
        output_dir="",
        results_prefixe="",
    ):
        """
        Initialize class
        """

        # Dictionary attributes
        self.dictionary_path = dictionary_file_path  # Khiops dictionary path
        self.dictionary_domain = kh.read_dictionary_file(dictionary_file_path)  # Khiops dictionary domain
        self.dictionary_name = dictionary_name  # Name of the dictionary to analyze
        self.root_table_path = data_table_path  # Path of the data table
        self.additional_table = additional_data_tables  # A dictionary containing the data paths and file paths for a multi-table dictionary file
        self.target = target_variable  # Name of the target variable
        self.unused_variable = []  # List of unused variable

        # Training attributes
        self.number_agregat = max_constructed_variables_per_variable  # Maximum number of variables to construct per native variable
        self.number_tree = max_trees  # Maximum number of trees to construct
        self.result_directory = results_dir  # Results directory path
        self.result_prefixe = results_prefixe  # Results prefix
        self.construction_rules = construction_rules  # List of constrcution rules to use

        self.output_dir = output_dir  # Output directory path
        self.discretisation = count_effect_reduction  # State of discretisation option

        # Other
        if exploration_type not in ["Variable", "All", "Primitive"]:
            print('Error : exploration_type must be "Variable", "All" or "Primitive" (default "Variable")')
            exit()
        self.exploration_type = exploration_type  # Exploration type
        self.pattern = r"[ ,.;`()]"  # to split deivation rule and extract variables and/or primitives list
        self.match_name_dictionary_variable_name = {}  # Dictionary to match dictionary name with its variable name
        self.match_dictionary_parent_dictionary = {} # Dictionary to match dictionary with its parent dictionary
        self.analyse_count = 0  # Real number of constructed variable
        self.table_exploration_array = (
            pd.DataFrame(  # Pandas dataframe to save tables information
                columns=[
                    "Table",
                    "Root",
                    "Variable number",
                    "Categorical variable number",
                    "Numerical variable number",
                    "Date variable number",
                ]
            )
        ) 

    def init_variable_importance_dictionary(self):
        """Initialize the dictionary to save the variables importance

        :return: empty dictionary
        :rtype: dict                
        """
        variable_importance_dictionary = {}
        return variable_importance_dictionary

    def init_importance_list_primitive(self):
        """Initialize the importance primitive list

        :return: Initial list of importance for primitive
        :rtype: list
        """
        importance_primitives = []
        for primitive in self.construction_rules:
            importance_primitives.append([primitive, 0])
        return importance_primitives
    
    def init_columns_names(self):
        """Initialize the columns names             
        """
        self.col_type = "type"
        self.col_level = "levelMT"
        self.col_agg = "levelMT aggregate"
        self.col_nb_agg = "real number of aggregates"
        self.col_importances_list = "importances list"
        self.col_level_no_discret = "importance"
        self.col_agg_no_discret = "importance aggregate"        

    def match_dictionary_name_variable_name(self):
        """
        Match each secondary dictionary name with its corresponding variable name in the parent dictionary
        and match each secondary dictionary name with its parent dictionary.
        """
        for dictionary in self.dictionary_domain.dictionaries:
            for variable in dictionary.variables:
                if variable.type == "Table" or variable.type == "Entity":
                    self.match_name_dictionary_variable_name[variable.name] = (
                        variable.object_type
                    )
                    self.match_dictionary_parent_dictionary[variable.object_type] = (
                        dictionary.name
                    )

    def get_grouping_variable(self):
        # Initialize python dictionary to match grouping intervals of count variable with tables names
        match_variable_to_add_table_name = {}
        
        # Train recoder on data
        train_reports_path, modeling_dictionary_path = kh.train_recoder(
            self.dictionary_path,
            self.dictionary_name,
            self.root_table_path,
            self.target,
            self.result_directory,
            additional_data_tables=self.additional_table,
            results_prefix=self.result_prefixe,
            informative_variables_only=False,
            max_constructed_variables=100,
            max_trees=self.number_tree,
            construction_rules=self.construction_rules,
        )
        preparation_report = kh.read_analysis_results_file(
            train_reports_path
        ).preparation_report
        # Add variable to select group
        for agregat in preparation_report.get_variable_names():
            split_agregat = re.split(self.pattern, agregat)
            if split_agregat[0] == "Count" and len(split_agregat) == 3:
                # # Add variable for selection
                IP_count_variable = (
                    kh.read_dictionary_file(modeling_dictionary_path)
                    .get_dictionary("R_" + self.dictionary_name)
                    .get_variable("IdP" + agregat)
                )
                P_count_variable = (
                    kh.read_dictionary_file(modeling_dictionary_path)
                    .get_dictionary("R_" + self.dictionary_name)
                    .get_variable("P" + agregat)
                )
                count_variable = (
                    kh.read_dictionary_file(modeling_dictionary_path)
                    .get_dictionary("R_" + self.dictionary_name)
                    .get_variable(agregat)
                )
                count = kh.Variable()
                count.name = count_variable.name
                count.type = "Numerical"
                count.used = False
                count.rule = "Sum(" + str(count_variable.rule) + ",0)"
                number_group = 1
                if (
                    preparation_report.get_variable_statistics(
                        count_variable.name
                    ).data_grid
                    is not None
                ):
                    number_group = len(
                        preparation_report.get_variable_statistics(count_variable.name)
                        .data_grid.dimensions[0]
                        .partition
                    )
                match_variable_to_add_table_name[
                    self.match_name_dictionary_variable_name[split_agregat[1]]
                ] = [IP_count_variable, P_count_variable, count, number_group]

        return match_variable_to_add_table_name

    def initialize_variable_state(self):
        """
        Initialize dictionary domain with all secondaries variables to unused
        """
        for dictionary in self.dictionary_domain.dictionaries:
            for variable in dictionary.variables:
                if variable.used == False:
                    self.unused_variable.append(variable.name)
                # Set secondaries table to unused
                if dictionary.root and (
                    variable.type == "Table" or variable.type == "Entity"
                ):
                    self.dictionary_domain.get_dictionary(dictionary.name).get_variable(
                        variable.name
                    ).used = False
                elif dictionary.root and (variable.name != self.target):
                    self.dictionary_domain.get_dictionary(dictionary.name).get_variable(
                        variable.name
                    ).used = False
                # Set secondaries variable to unused
                elif not dictionary.root:
                    self.dictionary_domain.get_dictionary(dictionary.name).get_variable(
                        variable.name
                    ).used = False

    def get_primitives_in_agregat(self, derivation_rule):
        """Get the primitives names in an agregat's derivation rule

        :param derivation_rule: Derivation rule
        :type derivation_rule: str
        :return: List of primitive use in the derivation rule
        :rtype: list
        """
        primitive_list = []  # Init a primitive list
        # Check if a primitive is in the derivation rule
        split_derivation_rule = re.split(self.pattern, derivation_rule)
        # 1- Check if the primitive is direcly present in the derivation rule
        for primitive in self.construction_rules:
            if primitive in split_derivation_rule:
                if primitive not in primitive_list:
                    primitive_list.append(primitive)

        # 2- Check if the primitive is a TableSelection rules and match with its corresponding construction rule
        for primitive in Dict_matching_partition.keys():
            if primitive in split_derivation_rule:
                if Dict_matching_partition[primitive] not in primitive_list:
                    primitive_list.append(Dict_matching_partition[primitive])
                if "TableSelection" not in primitive_list:
                    primitive_list.append("TableSelection")

        # 3- Check if the primitive name is present in the derivation rule
        # -> primitive name may be present instead of construction rule when
        # multiples primitives are used in the derivation rule
        for primitive in Dict_matching_rule_name.keys():
            if primitive in split_derivation_rule:
                if Dict_matching_rule_name[primitive] not in primitive_list:
                    primitive_list.append(Dict_matching_rule_name[primitive])
        return primitive_list

    def update_importance_list_primitive(self, primitive_importance, primitive_to_update, importance):
        """Update the primitive importance list according to new measured importances.

        :param primitive_to_update: List of primitive to be update
        :type primitive_to_update: list
        :param importance: Primitive importance
        :type importance: float
        """
        for i in range(len(primitive_importance)):
            if (
                primitive_importance[i][0] in primitive_to_update
                and primitive_importance[i][1] < importance
            ):
                primitive_importance[i][1] = importance
        return primitive_importance

    def get_importance(
        self, 
        dictionary, 
        variable, 
        variable_importance_dictionary,
        primitive_importance,
        selection_variable="", 
        selection_value="",
    ):
        """Get the importance measure of a variable by univariate analysis.

        :param dictionary: Khiops dictionary where the variable to estimate is.
        :param variable: Variable to estimate.
        :param variable_importance_dictionary: Variable importance dictionary to update.
        :param selection_variable: It trains with only the records such that the value of selection_variable is equal to selection_value, defaults to ""
        :type selection_variable: str, optional
        :param selection_value: See selection_variable option above, defaults to ""
        :type selection_value: str or int or float, optional
        :return variable_importance: Importance measure
        :rtype: float
        :return variable_importance_dictionary: Variable importance dictionary updated
        :rtype: dict   
        """
        # Initialize importance measure
        variable_importance = 0
        agregat_max = ""
        # Set the variable to estimate to used -> only the variable and its associated table is used
        self.dictionary_domain.get_dictionary(dictionary.name).get_variable(
            variable.name
        ).used = True
        # Create variables (agregats)
        train_reports_path, _ = kh.train_recoder(
            self.dictionary_domain,
            self.dictionary_name,
            self.root_table_path,
            self.target,
            self.result_directory,
            additional_data_tables=self.additional_table,
            results_prefix=(
                variable.name + "_" + dictionary.name + "_" + selection_value + "_"
            ),
            max_constructed_variables=self.number_agregat,
            max_trees=self.number_tree,
            construction_rules=self.construction_rules,
            selection_variable=selection_variable,
            selection_value=selection_value,
            keep_initial_categorical_variables=True,
            keep_initial_numerical_variables=True,
            informative_variables_only=False,
        )
        # Update importance measure -> importance measure is the maximum Khiops level in the agregat set.
        preparation_report = kh.read_analysis_results_file(
            train_reports_path
        ).preparation_report

        for agregat in preparation_report.variables_statistics:
            # if derivation rule is TableCount('Table') it is ignored because it doesn't contain the name of the variable
            if (
                agregat.derivation_rule
                == "TableCount("
                + get_key(self.match_name_dictionary_variable_name, dictionary.name)
                + ")"
                and agregat.level != 0
            ):
                continue
            # Get Variable importance -> maximum khiops level
            if self.exploration_type == "Variable" or self.exploration_type == "All":
                if agregat.level > variable_importance:
                    variable_importance = agregat.level
                    agregat_max = agregat.name
            # Get Primitive importance -> maximum khiops level
            if self.exploration_type == "Primitive" or self.exploration_type == "All":
                primitive_to_update = self.get_primitives_in_agregat(
                    agregat.derivation_rule
                )
                primitive_importance = self.update_importance_list_primitive(
                    primitive_importance, primitive_to_update, agregat.level
                )

        # Set the variable estimated to unused
        self.dictionary_domain.get_dictionary(dictionary.name).get_variable(
            variable.name
        ).used = False

        # Update variable importance dictionary
        key_table = dictionary.name
        key_var = variable.name
        var_type = variable.type
        var_level = variable_importance
        var_agg = agregat_max
        var_nb_agg = len(preparation_report.variables_statistics)
        if key_table not in variable_importance_dictionary.keys():
            stop
            self.create_new_variable(
                variable_importance_dictionary, 
                key_table, 
                key_var, 
                var_type,
                var_level,
                var_agg,
                var_nb_agg,
            )      
        else:
            if key_var not in variable_importance_dictionary[key_table].keys():
                self.create_new_variable(
                    variable_importance_dictionary, 
                    key_table, 
                    key_var, 
                    var_type,
                    var_level,
                    var_agg,
                    var_nb_agg,
                ) 
            else:
                if self.discretisation:
                    variable_importance_dictionary[key_table][key_var][self.col_importances_list].append(var_level)
                    if var_level > variable_importance_dictionary[key_table][key_var][self.col_level]:
                        self.update_existed_variable(
                                    variable_importance_dictionary, 
                                    key_table, 
                                    key_var, 
                                    var_type,
                                    var_level,
                                    var_agg,
                                    var_nb_agg,
                                )

        self.analyse_count = (
            len(preparation_report.variables_statistics) + self.analyse_count
        )
        return variable_importance_dictionary, primitive_importance

    def create_new_variable(
            self, 
            variable_importance_dictionary, 
            key_table, 
            key_var, 
            var_type,
            var_level,
            var_agg,
            var_nb_agg,
        ) :
        variable_importance_dictionary[key_table][key_var] = {}
        variable_importance_dictionary = self.fill_line(
            variable_importance_dictionary, 
            key_table, 
            key_var, 
            var_type,
            var_level,
            var_agg,
            var_nb_agg, 
            )
        if self.discretisation:
            variable_importance_dictionary[key_table][key_var][self.col_importances_list] = [var_level]
        return variable_importance_dictionary
    
    def update_existed_variable(
            self, 
            variable_importance_dictionary, 
            key_table, 
            key_var, 
            var_type,
            var_level,
            var_agg,
            var_nb_agg,
        ) :
        variable_importance_dictionary = self.fill_line(
            variable_importance_dictionary, 
            key_table, 
            key_var, 
            var_type,
            var_level,
            var_agg,
            var_nb_agg, 
            )
        return variable_importance_dictionary    

    def fill_line(
            self, 
            variable_importance_dictionary, 
            key_table, 
            key_var, 
            var_type,
            var_level,
            var_agg,
            var_nb_agg, 
        ) :
        variable_importance_dictionary[key_table][key_var][self.col_type] = var_type
        if self.discretisation:
            variable_importance_dictionary[key_table][key_var][self.col_level] = var_level
            variable_importance_dictionary[key_table][key_var][self.col_agg] = var_agg
        else:
            variable_importance_dictionary[key_table][key_var][self.col_level_no_discret] = var_level
            variable_importance_dictionary[key_table][key_var][self.col_agg_no_discret] = var_agg           
        variable_importance_dictionary[key_table][key_var][self.col_nb_agg] = var_nb_agg  
        return variable_importance_dictionary

    def add_variable(self, variable_to_add):
        """Adding khiops variable into dictionary domain

        :param variable_to_add: A list of variables to add into dictionary domain
        :type variable_to_add: list
        """
        for variable in variable_to_add:
            variable.used = False
            self.dictionary_domain.get_dictionary(self.dictionary_name).add_variable(
                variable
            )

    def remove_variable(self, variable_to_remove):
        """Remove khiops variable from dictionary domain

        :param variable_to_remove: A list of variables to remove from dictionary domain
        :type variable_to_remove: list
        """
        for variable in variable_to_remove:
            self.dictionary_domain.get_dictionary(self.dictionary_name).remove_variable(
                variable.name
            )

    def univariate_analysis(self):							   
        """
        Analyse variables by estimated a measure of importance for each variables using a
        n univariate analysis with discretisation.
        Create a  list of importance.
        """
        variable_importance_dictionary = self.init_variable_importance_dictionary()
        primitive_importance = self.init_importance_list_primitive()
        self.init_columns_names()

        # Get count grouping variable to add into dictionary domain -> depending of the table
        variable_to_add = self.get_grouping_variable()
        nb_tables = 0
        # Analyse by table
        for dictionary in self.dictionary_domain.dictionaries:
            print("dictionary : " + dictionary.name)
            if not dictionary.root:
                # Set the table to analyse to used
                
                self.dictionary_domain.get_dictionary(
                    self.match_dictionary_parent_dictionary[dictionary.name]
                ).get_variable(
                    get_key(self.match_name_dictionary_variable_name, dictionary.name)
                ).used = True
                variable_importance_dictionary[dictionary.name] = {}
				
                if self.discretisation:
                # Add variable for the selection -> only for Table type table
                    
                    if dictionary.name in variable_to_add.keys():
                        self.add_variable(variable_to_add[dictionary.name][:-1])
                        # Number of group to split instances
                        number_group = variable_to_add[dictionary.name][-1]
                        for i in range(number_group):
                            print("discretization : group " + str(i+1))
                            categorical_variable = 0
                            numerical_variable = 0
                            date_variable = 0
                            # Get importance measure for each variable of the table
                            for variable in tqdm(dictionary.variables):
                                # Get the number of variable type in tables
                                if variable.type == "Categorical":
                                    categorical_variable += 1
                                elif variable.type == "Numerical":
                                    numerical_variable += 1
                                else:
                                    date_variable += 1
                                if variable.name not in self.unused_variable:
                                    (
                                        variable_importance_dictionary, 
                                        primitive_importance, 
                                    ) = self.get_importance(
                                        dictionary,
                                        variable,
                                        variable_importance_dictionary,
                                        primitive_importance,
                                        selection_variable=variable_to_add[dictionary.name][0].name,
                                        selection_value="I" + str(i + 1),
                                    )

                         # Remove variable selection of the table
                        self.remove_variable(variable_to_add[dictionary.name][:-1])
                    # get unique importance list of Entity type table
                    else:
                        categorical_variable = 0
                        numerical_variable = 0
                        date_variable = 0
                        for variable in tqdm(dictionary.variables):
                            # Get the number of variable type in tables
                            if variable.type == "Categorical":
                                categorical_variable += 1
                            elif variable.type == "Numerical":
                                numerical_variable += 1
                            else:
                                date_variable += 1
                            if variable.name not in self.unused_variable:
                                (
                                    variable_importance_dictionary,
                                    primitive_importance,
                                ) = self.get_importance(
                                    dictionary, 
                                    variable, 
                                    variable_importance_dictionary, 
                                    primitive_importance,
                                )

                    # Set the table to analyse to unused
                    self.dictionary_domain.get_dictionary(
                        self.match_dictionary_parent_dictionary[dictionary.name]
                    ).get_variable(
                        get_key(self.match_name_dictionary_variable_name, dictionary.name)
                    ).used = False
                else:
                    # without count effect reduction
                    categorical_variable = 0
                    numerical_variable = 0
                    date_variable = 0
                    # Get importance measure for each variable of the table
                    for variable in tqdm(dictionary.variables):
                        # Get the number of variable type in tables
                        if variable.type == "Categorical":
                            categorical_variable += 1
                        elif variable.type == "Numerical":
                            numerical_variable += 1
                        else:
                            date_variable += 1
                        if variable.name not in self.unused_variable:
                            # Get variable and/or primitive importance
                            (
                                variable_importance_dictionary,
                                primitive_importance, 
                            ) = self.get_importance(
                                dictionary, 
                                variable, 
                                variable_importance_dictionary, 
                                primitive_importance,
                            )
                    
            else:
                categorical_variable = 0
                numerical_variable = 0
                date_variable = 0
                for variable in tqdm(dictionary.variables):
                    # Get the number of variable type in tables
                    if variable.type == "Categorical":
                        categorical_variable += 1
                    elif variable.type == "Numerical":
                        numerical_variable += 1
                    else:
                        date_variable += 1

            self.table_exploration_array.loc[nb_tables] = [
                dictionary.name,
                dictionary.root,
                len(dictionary.variables),
                categorical_variable,
                numerical_variable,
                date_variable,
            ]
            nb_tables += 1

        return (
            variable_importance_dictionary, 
            primitive_importance
        )

    def variables_analysis(self):
        """Global function to estimate variable's importance"""
        # Get matching dictionary for variable table name and table name
        self.match_dictionary_name_variable_name()
        # Initialise dictionary domain -> all variable to unused
        self.initialize_variable_state()

        (
            variable_importance_dictionary, 
            primitive_importance,
        ) = self.univariate_analysis()

        if self.exploration_type == "Variable" or self.exploration_type == "All":
            variable_exploration_file_name = ("variable_exploration.json")
            variable_exploration_file = os.path.join(self.output_dir, variable_exploration_file_name)
            with open(variable_exploration_file, "w") as f:
                json.dump(variable_importance_dictionary, f, indent=2)

        if self.exploration_type == "Primitive" or self.exploration_type == "All":
            primitive_exploration_file_name = ("primitive_exploration.json")
            primitive_exploration_file = os.path.join(self.output_dir, primitive_exploration_file_name)
            with open(primitive_exploration_file, "w") as f:
                json.dump(primitive_importance, f, indent=2)

        if self.exploration_type == "All":
            return variable_importance_dictionary, primitive_importance
        elif self.exploration_type == "Variable":
            return variable_importance_dictionary
        elif self.exploration_type == "Primitive":
            return primitive_importance



