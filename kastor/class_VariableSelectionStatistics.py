import os
import pandas as pd
import sys
import json
from khiops import core as kh

# Ajouter le chemin du dossier parent
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "")))


class VariableSelectionStatistics:
    """
    """
    def __init__(
            self,
            dictionary_file_path,
            dictionary_name,
            exploration_type="Variable",
            count_effect_reduction=True,
            output_dir="",
            variable_exploration_name="variable_exploration.json",
            primitive_exploration_name="primitive_exploration.json",
        ):
        """
        Initialize class
        """
        self.dictionary_file_path = dictionary_file_path
        self.dictionary_name = dictionary_name
        self.exploration_type = exploration_type
        self.discretisation = count_effect_reduction  # State of discretisation option
        self.output_dir = output_dir  # Output directory path
        self.variable_exploration_name = variable_exploration_name
        self.primitive_exploration_name = primitive_exploration_name
        self.init_variables()

    def init_variables(self):
        # variables names for variables and primitives exploration
        self.table = "table"
        self.variable = "variable"
        self.col_type = "type"
        self.col_level = "levelMT"
        self.col_agg = "levelMT aggregate"
        self.col_nb_agg = "real number of aggregates"
        self.col_importances_list = "importances list"
        self.col_primitive = "primitive"
        self.col_level_no_discret = "importance"
        self.col_agg_no_discret = "importance aggregate"   

    def init_df_variable_exploration(self, nb_columns):
        """Initialize the pandas dataframe to save the variables exploration array 

        :return: empty pandas dataframe
        :rtype: dataframe                
        """
        # with count effect reduction
        # format 5 colonnes : type, levelMT, levelMT aggregate, real number of aggregates, importances list
        if nb_columns == 5:
            df_variable_exploration = (
                pd.DataFrame(  # Pandas dataframe to save variables information
                    [],
                    columns=[
                        self.table,
                        self.variable,
                        self.col_type,
                        self.col_level,
                        self.col_agg,
                        self.col_nb_agg,
                        self.col_importances_list,
                    ],
                )
            )
        # without count effect reduction
        # format 4 colonnes : type, importance, importance aggregate, real number of aggregates
        elif nb_columns == 4:
            self.discretisation = False
            df_variable_exploration = (
                pd.DataFrame(  # Pandas dataframe to save variables information
                    [],
                    columns=[
                        self.table,
                        self.variable,
                        self.col_type,
                        self.col_level_no_discret,
                        self.col_agg_no_discret,
                        self.col_nb_agg,
                    ],
                )
            )
        else:
            print("Warning : dictionary in json file doesn't respect the expected format")

        return df_variable_exploration

    def write_txt_report(
            self, 
            df_variable_exploration = "",
            df_primitive_exploration = "",
        ):
        """Create a text file with univariate analysis results"""
        if self.exploration_type == "All":
            exploration_file_name = "variable_and_primitive"
        elif self.exploration_type == "Variable":
            exploration_file_name = "variable"
        elif self.exploration_type == "Primitive":
            exploration_file_name = "primitive"
        exploration_file_name += "_exploration.txt"
        exploration_file = open(os.path.join(self.output_dir, exploration_file_name), "w")
        """
        exploration_file.write(100 * "=" + "\n")
        exploration_file.write(
            "Variable and/or primitive exploration information\n"
        )
        exploration_file.write(100 * "=" + "\n")
        exploration_file.write(
            f"Agregats number per variable : \t {self.number_agregat} \n"
        )
        exploration_file.write(
            f"Real analyse agregats number : \t {self.analyse_count} \n"
        )
        exploration_file.write(f"Discretisation : \t {self.discretisation} \n")
        exploration_file.write(f"Exploration for : \t {self.exploration_type} \n")
        exploration_file.write("\n")
        exploration_file.write(100 * "=" + "\n")
        exploration_file.write("Table exploration information\n")
        exploration_file.write(100 * "=" + "\n")
        exploration_file.write(self.table_exploration_array.to_markdown())
        exploration_file.write("\n")
        exploration_file.write("\n")
        exploration_file.write("\n")
        """
        if self.exploration_type == "All" or self.exploration_type == "Variable":        
            exploration_file.write(180 * "=" + "\n")
            exploration_file.write("Importance measures for variables \n\n")
            exploration_file.write(df_variable_exploration.to_markdown()+ "\n")
            exploration_file.write(180 * "=" + "\n")
            exploration_file.write("\n")
            exploration_file.write("\n")

        if self.exploration_type == "All" or self.exploration_type == "Primitive":
            exploration_file.write(39 * "=" + "\n")
            exploration_file.write("Importance measures for primitives \n\n")
            exploration_file.write(df_primitive_exploration.to_markdown()+ "\n")
            exploration_file.write(39 * "=" + "\n")
            exploration_file.write("\n")
            exploration_file.write("\n")

        exploration_file.write(80 * "=" + " END " + 80 * "=" + "\n")
        exploration_file.close()

    def read_data(self, variable_exploration_name, primitive_exploration_name):
        """
        read univariate mulitable analysis results
        """ 
        # default value to return if exploration_type = "Variable" or "Primitive"
        variable_importance_dictionary = {}
        primitive_importance = []

        # reading files if existing
        variable_exploration_file_name = (variable_exploration_name)
        variable_exploration_file = os.path.join(self.output_dir, variable_exploration_file_name)

        if os.path.exists(variable_exploration_file):
            with open(variable_exploration_file, "r") as f:
                variable_importance_dictionary = json.load(f)
        else:
            print("file " + '"' + variable_exploration_file  + '"'  + " doesn't exist")

        primitive_exploration_file_name = (primitive_exploration_name)
        primitive_exploration_file = os.path.join(self.output_dir, primitive_exploration_file_name)
        if os.path.exists(primitive_exploration_file):
            with open(primitive_exploration_file, "r") as f:
                primitive_importance = json.load(f)  
        else:
            print("file " + '"' + primitive_exploration_file  + '"'  + " doesn't exist")

        return variable_importance_dictionary, primitive_importance  
    
    def get_df_variable_importance(self, variable_importance_dictionary):
        """
        convert dict to pandas dataframe
        """ 
        for key1,value1 in variable_importance_dictionary.items():
            for key2,value2 in variable_importance_dictionary[key1].items():
                nb_columns = len(value2)
                break
            break
         
        df_variable_exploration = self.init_df_variable_exploration(nb_columns)
        i=0
        if self.discretisation:
            for key1,value1 in variable_importance_dictionary.items():
                for key2,value2 in variable_importance_dictionary[key1].items():
                    df_variable_exploration.loc[i] = [
                        key1,
                        key2,
                        value2[self.col_type],
                        value2[self.col_level],
                        value2[self.col_agg],
                        value2[self.col_nb_agg],
                        value2[self.col_importances_list],
                    ]
                    i+=1   
        else:
            for key1,value1 in variable_importance_dictionary.items():
                for key2,value2 in variable_importance_dictionary[key1].items():
                    df_variable_exploration.loc[i] = [
                        key1,
                        key2,
                        value2[self.col_type],
                        value2[self.col_level_no_discret],
                        value2[self.col_agg_no_discret],
                        value2[self.col_nb_agg],
                    ]
                    i+=1             
        return df_variable_exploration

    def get_df_primitive_importance(self, primitive_importance):
        """
        convert list to pandas dataframe
        """        
        return pd.DataFrame(primitive_importance, columns=[self.col_primitive,self.col_level])
    
    def get_list_variable(self, df_variable_exploration):
        """
        list variables
 
        :param df_variable_exploration: univariate analysis results
        :type df_variable_exploration: pandas dataframe 
        :return: list of variables
        :rtype: list         
        """ 
        return (df_variable_exploration[self.variable].tolist())

    def get_variable_number_zero_level(self, df_variable_exploration):
        """
        number of variables with level zero

        :param df_variable_exploration: univariate analysis results
        :type df_variable_exploration: pandas dataframe 
        :return: number of variables with level zero
        :rtype: int 
        """ 
        if self.discretisation:
            return len(df_variable_exploration[df_variable_exploration[self.col_level]==0])
        else:
            return len(df_variable_exploration[df_variable_exploration[self.col_level_no_discret]==0])
    
    def filter_variables_in_dico_domain(self, df_variable_exploration, nb_var_to_filter=None):
        """
        filter the nb_var_to_filter variables with low levelMT in dictionary
        if nb_var_to_filter is None filter all varaibles with levelMT zero

        :param df_variable_exploration: univariate analysis results
        :type df_variable_exploration: pandas dataframe        
        :param nb_var_to_filter: number of variables to filter, default None
        :type nb_var_to_filter: int
        :return: khiops dictionary
        :rtype: dictionary_domain     
        """ 
        # copie du dico pour créer le dico avec variables Unused
        dictionary_domain_10 = (kh.read_dictionary_file(self.dictionary_file_path))
        dictionary_domain_10_reduced = dictionary_domain_10.copy()
        mypath, myfile = os.path.split(self.dictionary_file_path)
        # indices des noms des colonnes de df_variable_exploration
        col_list = list(df_variable_exploration.columns)
        indice_table = col_list.index(self.table)
        indice_variable = col_list.index(self.variable)
        if self.discretisation:
            indice_level = col_list.index(self.col_level)
        else:
            indice_level = col_list.index(self.col_level_no_discret)

        if nb_var_to_filter == None:
              for row in df_variable_exploration.itertuples(index=False):
                level = row[indice_level]
                if level == 0:
                    var_to_unselect = row[indice_variable]
                    table = row[indice_table]               
                    dictionary_domain_10_reduced = self.unselect_var(
                        dictionary_domain_10_reduced, 
                        table, 
                        var_to_unselect
                    )
        else:
            i=0
            for row in df_variable_exploration.itertuples(index=False):
                level = row[indice_level]
                if i < nb_var_to_filter :
                    i += 1
                    var_to_unselect = row[indice_variable]
                    table = row[indice_table]
                    dictionary_domain_10_reduced = self.unselect_var(
                        dictionary_domain_10_reduced, 
                        table, 
                        var_to_unselect
                    )

        # écriture du dictionnaire
        if nb_var_to_filter == None:
            str_nb = 'level0'
        else:
            str_nb = 'filter' + str(nb_var_to_filter)
        dictionary_file_path_reduced = os.path.join(
            mypath, 
            "reduced_dictionary_" + str_nb + ".kdic"
        )
        dictionary_domain_10_reduced.export_khiops_dictionary_file(dictionary_file_path_reduced)
        print("Writing dictionary : " + dictionary_file_path_reduced)
        return dictionary_domain_10_reduced
    
    def unselect_var(self, dictionary_domain, table, var_to_unselect):
        flag = False
        for dico in dictionary_domain.dictionaries:
            if not dico.root:
                if dico.name == table:
                    for var in dico.variables:
                        if var.name == var_to_unselect:
                            if not dico.is_key_variable(var):
                                var.used = False
                                flag = True
                                break
        if not flag:
            print("Warning : the variable " + '"' + var_to_unselect + '"' + 
                  " in " + '"' +  table + '"' + " dictionary doesn't exist in the dictionary")
        return dictionary_domain
    
    def get_variables_analysis(self):
        """
        get variable importance and primitive importance json file
        convert into dataframes

        :return: variables importances
        :rtype: dataframe   
        :return: primitives importances
        :rtype: dataframe   
        """ 
        variable_importance_dictionary, primitive_importance = self.read_data(
            self.variable_exploration_name, 
            self.primitive_exploration_name,
        )
        if variable_importance_dictionary != {} :
            df_variable_exploration = self.get_df_variable_importance(variable_importance_dictionary)
        if primitive_importance != []:
            df_primitive_exploration = self.get_df_primitive_importance(primitive_importance)

        if variable_importance_dictionary != {} and primitive_importance != []:
            return df_variable_exploration, df_primitive_exploration
        elif variable_importance_dictionary != {}:
            return df_variable_exploration
        elif primitive_importance != []:
            return df_primitive_exploration
