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
import json
import matplotlib.pyplot as plt
import pandas as pd
from os import path
from sys import exit
from matplotlib.backends.backend_pdf import PdfPages
from math import floor

from kastor._timeevalscore import ProactiveEvalScore, ReactiveEvalScore
from kastor._util import (
    exist,
    parse_name_file,
    work_path,
)


class TemporalEvaluator:
    """Classe pour évaluer le modèle.


    """

    def __init__(
        self,
        data_tables,
        temporal_parameters,
        sep="\t",
        mobile=True,
        path_suffix = "",
        result_suffix = "",
    ):
        self.data_tables = data_tables
        self.temporal_parameters = temporal_parameters
        self.sep = sep
        self.mobile = mobile
        self.path_suffix = path_suffix
        self.result_suffix = result_suffix

                

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
        eval_react_json = file_to_write + ".json"
        return eval_react_json

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
        eval_pro_json = file_to_write + ".json"
        return eval_pro_json

    def evaluate(self, score_by_period_unit, target_by_period_unit):
        """
        Evaluation
            | Récupération de la table des scores et cibles journaliers
            | Deux métriques d'évaluation : précision et rappel
        """
        name_var_id = self.data_tables["main_table"]["key"]
        
        if self.mobile:
            try:
                target_duration = self.temporal_parameters["target_duration"]
            except KeyError:
                target_duration = 1
        else:
            target_duration = 1
        
        period_unit = self.temporal_parameters["period_unit"]
        # start_date    
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
        
        nb_targets = period_nb + target_duration - 1
        nb_scores = period_nb
        """
        if period_nb <= target_duration:
            print(
                "Pour avoir suffisamment de données pour l'évaluation "
                + "on doit avoir period_nb > target_duration"
            )
            exit()
        """
        if self.result_suffix != "":
            result_suffix = "_" + self.result_suffix

        # récupération du répertoire
        file_main = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_main)
        rep_result = work_path(rep, self.mobile, self.path_suffix)

        # concatenation df_score et df_target si les dataframes existent
        # sinon chargement des fichiers s'ils existent
        if type(score_by_period_unit) == str:
            # chargement du fichier score by period unit
            file_score = path.join(
                rep_result, 
                "score_by_period_unit" + result_suffix + ".csv"
            )
            exist(file_score)
            print("Scores : fichier " + file_score)
            df_score = pd.read_csv(
                file_score, sep=self.sep, encoding="ISO-8859-1"
            )
        else : 
            df_score = score_by_period_unit

        if type(target_by_period_unit) == str:
            # chargement du fichier target by period unit
            file_target = path.join(
                rep_result, 
                "target_by_period_unit" + result_suffix + ".csv"
            )
            exist(file_target)
            print("Targets : fichier " + file_target)
            df_target = pd.read_csv(
                file_target, sep=self.sep, encoding="ISO-8859-1"
            )
        else : 
            df_target = target_by_period_unit
            
        # si toutes les cibles sont nulles on arrête le programme
        df_target_sum = df_target.drop(name_var_id, axis=1)
        target_sum = (df_target_sum.sum(axis=1)).sum()
        
        if target_sum == 0:
            print("Toutes les cibles sont nulles sur la période sélectionnée, "
                  + "l'évaluation ne peut pas être effectuée"
              )
            exit()
            
        df_pivot = pd.merge(df_target, df_score, how="inner", on=name_var_id)
        
        # evaluations reactives et proactives

        i_bin = 20  # liste des pct de target analyse
        i_eval_duration = period_nb
        """
        i_eval_duration = min(
            period_nb, 30
        )  # duree en nombre de jours analyse
        """
        i_nb_target = (
            nb_targets  # nombre de colonnes de cibles dans le fichier
        )
        i_nb_score = nb_scores  # nombre de colonnes de scores dans le fichier
        id_position = 0  # position colonne de l id
        param_eval_reac = (
            i_bin,
            i_eval_duration,
            i_nb_target,
            i_nb_score,
            id_position,
            target_duration,
        )
        eval_react_json = TemporalEvaluator._evaluation_reactif_df(
            param_eval_reac,
            df_pivot,
            path.join(rep_result, "eval_reactif" + result_suffix),
        )

        """
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
            target_duration,
        )
        eval_pro_json = TemporalEvaluator._evaluation_proactif_df(
            param_eval_pro,
            df_pivot,
            path.join(rep_result, "eval_proactif"),
        )
        return eval_react_json, eval_pro_json
        """
        return eval_react_json

    def plot(
            self, 
            eval_react_json, 
            eval_pro_json='', 
            list_metric=["gain", "rappel"],
        ):
        """
        Représentations graphiques des courbes, par déciles de top scores,
        des deux métriques : précision et rappel
        """
        if self.result_suffix != "":
            result_suffix = "_" + self.result_suffix
  
        # récupération du répertoire où sont écrits les json
        for eval_file_json in [eval_react_json, eval_pro_json] :
            rep, file_ext = path.split(eval_file_json)
            file, ext = parse_name_file(file_ext)
            
            try: 
                with open(eval_file_json, "r") as json_file:
                    data = json.load(json_file)
            except FileNotFoundError:
                continue
            
            #list_metric = ["precision", "gain", "precision moyenne",
            #               "rappel", "precision globale"]
            
            pp = PdfPages(
                path.join(rep, file + ' - courbes métriques.pdf')
            )
            
            for metric in list_metric :    

                try:
                    dict_json_file = data[metric]
                except KeyError:
                    print(
                        "la métrique '" + metric + "' n'est pas présente"
                        + " dans les résultats"
                    )
                    continue
                    
                for key, value in dict_json_file.items():
                    dict_json_file[key] = float(value)

                x, y = zip(
                    *dict_json_file.items()
                )  # unpack a list of pairs into two tuples
                    
                fig = plt.figure(figsize=(5,3))
                plt.plot(x, y)
                plt.title(file + ' - ' + metric)
                plt.xlabel("Top scores")
                plt.ylabel(metric)
                plt.xticks(rotation=45)
                plt.gca().yaxis.set_tick_params(labelsize = 8)
                plt.gca().xaxis.set_tick_params(labelsize = 8)
                fig.savefig(
                    path.join(
                        rep, 
                        file + ' - ' + metric + result_suffix + ".png"
                    )
                )
                plt.show()
                pp.savefig(fig)
        pp.close()
                
    def plot_all(self, list_path, list_metric=["gain", "rappel"]):
        """
        récupération des paths des courbes eval_reactif et eval_proactif 
        tracé des courbes précision et rappel
        """
        if self.result_suffix != "":
            result_suffix = "_" + self.result_suffix
            
        # récupération du répertoire principal pour écriture du png
        file_target = self.data_tables["main_table"]["file_name"]
        rep, file = path.split(file_target)

        #list_metric = ["precision", "gain", "precision moyenne",
        #               "rappel", "precision globale"]

        
        pp = PdfPages(
            path.join(rep,'courbes_superposees_metriques.pdf')
            )

        for type_eval in ["reactif", "proactif"]:
            for metric in list_metric:
                
                # pour récupérer le découpage seulement (en vingtile ou décile...)
                mypath = list_path[0]
                eval_json_file = path.join(
                        rep, 
                        mypath,
                        'eval_' + type_eval + result_suffix + '.json'
                    )
                try:
                    with open(eval_json_file, 'r') as json_file:
                        data = json.load(json_file)
                except FileNotFoundError:
                    continue
                    
                try:
                    dict_json_file = data[metric]
                except KeyError:
                    print(
                        "la métrique '" + metric + "' n'est pas présente"
                        + " dans les résultats"
                    )
                    continue
                
                for cle, valeur in dict_json_file.items():
                    dict_json_file[cle] = float(valeur)
                # unpack a list of pairs into two tuples
                x, y =(zip(*dict_json_file.items())) 
                df_all = pd.DataFrame((x), columns=['Top scores'])                
                
                
                # récupération des valeurs des métriques pour chacun des modèles 
                for mypath in list_path:
                    eval_json_file = path.join(
                            rep, 
                            mypath,
                            'eval_' + type_eval + result_suffix + '.json'
                        )
                    try:
                        with open(eval_json_file, 'r') as json_file:
                            data = json.load(json_file)    
                    except FileNotFoundError:
                        continue
                            
                    try:
                        dict_json_file = data[metric]
                    except KeyError:
                        print(
                            "la métrique '" + metric + "' n'est pas présente"
                            + " dans les résultats"
                        )
                        continue
                
                    for cle, valeur in dict_json_file.items():
                        dict_json_file[cle] = float(valeur)
                    # unpack a list of pairs into two tuples
                    x, y =(zip(*dict_json_file.items()))                       
                    
                    df = pd.DataFrame({'Top scores': x, mypath:y})  
                    df_all = pd.merge(df_all, df)

                # représentation graphique
                fig = plt.figure(figsize=(5,3))
                plt.title(metric)
                plt.xlabel('Top scores')
                plt.ylabel(metric)
                plt.xticks(rotation=45)
                plt.gca().yaxis.set_tick_params(labelsize = 8)
                plt.gca().xaxis.set_tick_params(labelsize = 8)
                list_legend = []

                for mypath in list_path:        
                    plt.plot(df_all['Top scores'], df_all[mypath])
                    list_legend.append(mypath)
                
                plt.legend(list_legend)
                
                fig.savefig(
                    path.join(
                        rep, 
                        metric + '_test_graphiques_superposes.png'
                        ), 
                    bbox_inches="tight"
                    )
                plt.show()
                pp.savefig(fig)
        pp.close()


