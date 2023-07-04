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
Module d'évaluation des scores temporels.
"""
import io
import json
import numpy as np
import pandas as pd


class ProactiveEvalScore(object):
    """Classe pour évaluer les scores temporels en **proactif**.

    Parameters
    ----------
    list_bin_target : list, optional, default [x * 0.1 for x in range(1, 10)]
        liste des pourcentages de target à analyser
    i_eval_duration : int, optional, default 30
        durée en nombre de jours de l' analyse
    i_nb_target : int, optional, default nb_targets
        nombre de colonnes de cibles dans le fichier
    i_nb_score : int, optional, default nb_scores
        nombre de colonnes de scores dans le fichier
    id_position : int, optional, default 0
        position colonne de l'id
    """

    def __init__(self, param_eval, score_data=None, **kwargs):
        (
            list_bin_target,
            i_eval_duration,
            i_nb_target,
            i_nb_score,
            id_position,
        ) = param_eval

        self.list_bin_target = (
            list_bin_target  # liste des pourcentages de target à analyser
        )
        self.i_eval_duration = (
            i_eval_duration  # durée en nombre de jours de l' analyse
        )
        self.i_nb_target = (
            i_nb_target  # nombre de colonnes de cibles dans le fichier
        )
        self.i_nb_score = (
            i_nb_score  # nombre de colonnes de scores dans le fichier
        )
        self.id_position = id_position  # position colonne de l'id

        self.i_latency = kwargs.get("latency", 7)  # latence des jours

        # Return empty report if no JSON data or no basic information available
        if score_data is None:
            return

        # calcul de l evaluation

        self.i_nb_row = score_data.shape[0]
        self.i_nb_col = score_data.shape[1]
        self.i_bin = len(self.list_bin_target)

        self.list_target_j1 = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]
        self.list_target_sum_with_latency = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]
        self.list_target_id_sum = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]
        self.list_target_id_sum_cum = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]

        self.list_a_var = score_data.columns.values
        self.id_name = self.list_a_var[self.id_position]
        list_target_id = [np.zeros(0)] * self.i_bin
        list_target_id_lost = [np.zeros(0)] * self.i_bin
        self.nb_target_total = (
            score_data[
                self.list_a_var[1 : self.i_eval_duration + self.i_latency]
            ]
            .sum()
            .sum()
        )

        for itile, val in enumerate(self.list_bin_target):
            npcttarget = self.list_bin_target[itile]
            pctfirst = npcttarget * 1 / self.i_eval_duration
            i_nb_row_2 = int(self.i_nb_row * pctfirst)

            data_f = score_data
            for i_pos_day in range(0, self.i_eval_duration):
                if i_pos_day == 0:
                    data_f.sort_values(
                        by=self.list_a_var[i_pos_day + 1 + self.i_nb_target],
                        inplace=True,
                        ascending=False,
                    )
                    data_f_2 = data_f.iloc[0 : i_nb_row_2 - 1, :]
                else:
                    data_f = data_f.iloc[i_nb_row_2:, :].copy()
                    data_f.sort_values(
                        by=self.list_a_var[i_pos_day + 1 + self.i_nb_target],
                        inplace=True,
                        ascending=False,
                    )
                    data_f_2 = data_f.iloc[0 : i_nb_row_2 - 1, :]

                val = data_f_2.iloc[:, i_pos_day + 1].sum()
                id_0 = data_f_2[
                    data_f_2[
                        self.list_a_var[
                            i_pos_day + 1 : i_pos_day + 1 + self.i_latency
                        ]
                    ].max(axis=1)
                    == 1
                ][self.id_name].unique()
                if i_pos_day == 0:
                    list_target_id[itile] = id_0
                    val_2 = len(id_0)
                    self.list_target_id_sum[itile][i_pos_day] = len(id_0)
                else:
                    if i_pos_day == 1:
                        id_lost = data_f_2[
                            data_f_2[self.list_a_var[1:i_pos_day]].max(axis=1)
                            == 1
                        ][self.id_name].unique()
                        list_target_id_lost[itile] = id_lost
                    id_1 = np.concatenate(
                        (id_0, list_target_id[itile]), axis=None
                    )
                    list_target_id[itile] = np.unique(id_1)
                    val_2 = len(id_0)
                    self.list_target_id_sum[itile][i_pos_day] = len(
                        list_target_id[itile]
                    )
                self.list_target_j1[itile][i_pos_day] = val
                self.list_target_sum_with_latency[itile][i_pos_day] = val_2

        list_res = [0] * self.i_bin
        for i in range(0, self.i_bin):
            list_res[i] = 0
            for i_pos_day in range(0, self.i_eval_duration):
                list_res[i] = list_res[i] + self.list_target_j1[i][i_pos_day]

        for i in range(0, self.i_bin):
            list_res[i] = 0
            for i_pos_day in range(0, self.i_eval_duration):
                list_res[i] = (
                    list_res[i]
                    + self.list_target_sum_with_latency[i][i_pos_day]
                )

        for i in range(0, self.i_bin):
            list_res[i] = 0
            for i_pos_day in range(0, self.i_eval_duration):
                list_res[i] = (
                    list_res[i] + self.list_target_id_sum[i][i_pos_day]
                )

    def eval_score_file(self, param_eval, score_file_name, **kwargs):
        """Constructs an instance from a Khiops JSON file.

        .. note:: Returns an empty instance if a parsing error occurs or
            if there are missing mandatory attributes in the JSON file.
        """
        # Even valid Khiops JSON files may fail to load when containing unicode
        # chars (due to python decoder)
        with io.open(score_file_name, "r", encoding="utf-8") as scoreFile:
            try:
                score_data = pd.read_csv(score_file_name, sep=";")
            except Exception as error:
                score_data = None
                print(
                    "error in loading Eval Score file "
                    + score_file_name
                    + " ("
                    + str(error)
                    + ")"
                )

        # Initialize from JSON data
        self.__init__(param_eval, score_data, **kwargs)

    def eval_score_df(self, param_eval, score_df, **kwargs):
        """Construction d'une instance `ProactiveEvalScore` à partir du
        dataframe comportant la table pivot."""
        score_data = score_df
        self.__init__(param_eval, score_data, **kwargs)

    def write_report_file(self, file_name):
        """Writes the instance's TSV report to the specified path."""
        with io.open(file_name, "w", encoding="utf-8") as report_file:
            report_file.write("nb individus : \t" + str(self.i_nb_row) + "\n")
            report_file.write(
                "nb targets total : \t" + str(self.nb_target_total) + "\n"
            )
            report_file.write(
                "pourcentage target : \t"
                + str(self.nb_target_total / self.i_nb_row)
                + "\n"
            )
            report_file.write("latency : \t" + str(self.i_latency) + "\n")

            if self.nb_target_total > 0:
                for itile, val in enumerate(self.list_bin_target):
                    report_file.write(
                        "precision/rappel pour TARGET de "
                        + str(format(val, ".2f"))
                        + " : \t"
                        + str(
                            self.list_target_id_sum[itile][
                                self.i_eval_duration - 1
                            ]
                            / (val * self.i_nb_row)
                        )
                        + "\t"
                        + str(
                            self.list_target_id_sum[itile][
                                self.i_eval_duration - 1
                            ]
                            / self.nb_target_total
                        )
                        + "\n"
                    )

    def write_report_file_json(self, file_name):
        """Writes the instance's Json report to the specified path."""
        dict_json_file = {}
        dict_json_file["nb individus"] = str(self.i_nb_row)
        dict_json_file["nb targets total"] = str(self.nb_target_total)
        dict_json_file["pourcentage target"] = str(
            self.nb_target_total / self.i_nb_row
        )
        dict_json_file["latency"] = str(self.i_latency)
        dict_json_file_precision = {}
        dict_json_file_recall = {}

        if self.nb_target_total > 0:
            for itile, val in enumerate(self.list_bin_target):
                dict_json_file_precision[str(format(val, ".2f"))] = str(
                    self.list_target_id_sum[itile][self.i_eval_duration - 1]
                    / (val * self.i_nb_row)
                )
                dict_json_file_recall[str(format(val, ".2f"))] = str(
                    self.list_target_id_sum[itile][self.i_eval_duration - 1]
                    / self.nb_target_total
                )

            dict_json_file["precision"] = dict_json_file_precision
            dict_json_file["rappel"] = dict_json_file_recall

        with io.open(file_name, "w", encoding="utf-8") as report_file:
            json.dump(dict_json_file, report_file, indent=4)


class ReactiveEvalScore(object):
    """Classe pour évaluer les scores temporels en **réactif**.

    Parameters
    ----------
    i_bin : int, optional, default 20
        nombre de pourcentage de target à analyser
    i_eval_duration : int, optional, default 30
        durée en nombre de jours de l' analyse
    i_nb_target : int, optional, default nb_targets
        nombre de colonnes de cibles dans le fichier
    i_nb_score : int, optional, default nb_scores
        nombre de colonnes de scores dans le fichier
    id_position : int, optional, default 0
        position colonne de l'id
    """

    def __init__(self, param_eval, score_data=None, **kwargs):
        (
            i_bin,
            i_eval_duration,
            i_nb_target,
            i_nb_score,
            id_position,
        ) = param_eval

        self.i_bin = i_bin  # liste des pourcentages de target à analyser
        self.i_eval_duration = (
            i_eval_duration  # durée en nombre de jours de l'analyse
        )
        self.i_nb_target = (
            i_nb_target  # nombre de colonnes de cibles dans le fichier
        )
        self.i_nb_score = (
            i_nb_score  # nombre de colonnes de scores dans le fichier
        )
        self.id_position = id_position  # position colonne de l'id

        self.i_latency = kwargs.get("latency", 1)  # latence des jours

        # Return empty report if no JSON data or no basic information available
        if score_data is None:
            return

        # calcul de l evaluation

        self.i_nb_row = score_data.shape[0]
        self.i_nb_col = score_data.shape[1]

        self.list_target_j1 = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]
        self.list_target_sum_with_latency = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]
        self.list_target_id_sum = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]
        self.list_target_id_sum_cum = [
            [0 for i in range(self.i_eval_duration)] for j in range(self.i_bin)
        ]

        self.list_a_var = score_data.columns.values
        self.id_name = self.list_a_var[self.id_position]
        list_target_id = [np.zeros(0)] * self.i_bin
        self.nb_target_total = (
            score_data[
                self.list_a_var[1 : self.i_eval_duration + self.i_latency]
            ]
            .sum()
            .sum()
        )

        for i_pos_day in range(0, self.i_eval_duration):
            score_data.sort_values(
                by=self.list_a_var[i_pos_day + 1 + self.i_nb_target],
                inplace=True,
                ascending=False,
            )

            for itile in range(0, self.i_bin):
                i_nb_row_2 = int(self.i_nb_row * (itile + 1) / self.i_bin)
                data_f_2 = score_data.iloc[0 : i_nb_row_2 - 1, :]
                val = data_f_2.iloc[:, i_pos_day + 1].sum()
                id_0 = data_f_2[
                    data_f_2[
                        self.list_a_var[
                            i_pos_day + 1 : i_pos_day + 1 + self.i_latency
                        ]
                    ].max(axis=1)
                    == 1
                ][self.id_name].unique()
                if i_pos_day == 0:
                    list_target_id[itile] = id_0
                    self.list_target_id_sum[itile][i_pos_day] = len(id_0)
                else:
                    id_1 = np.concatenate(
                        (id_0, list_target_id[itile]), axis=None
                    )
                    list_target_id[itile] = np.unique(id_1)
                    self.list_target_id_sum[itile][i_pos_day] = len(
                        list_target_id[itile]
                    )
                self.list_target_j1[itile][i_pos_day] = val

        list_res = [0] * self.i_bin
        for i in range(0, self.i_bin):
            list_res[i] = 0
            for i_pos_day in range(0, self.i_eval_duration):
                list_res[i] = list_res[i] + self.list_target_j1[i][i_pos_day]

        for i in range(0, self.i_bin):
            list_res[i] = 0
            for i_pos_day in range(0, self.i_eval_duration):
                list_res[i] = (
                    list_res[i] + self.list_target_id_sum[i][i_pos_day]
                )

    def eval_score_file(self, param_eval, score_file_name, **kwargs):
        """Constructs an instance from a Khiops JSON file.

        .. note:: Returns an empty instance if a parsing error occurs or
            if there are missing mandatory attributes in the JSON file.
        """
        # Even valid Khiops JSON files may fail to load when containing unicode
        # chars (due to python decoder)
        with io.open(score_file_name, "r", encoding="utf-8") as scoreFile:
            try:
                score_data = pd.read_csv(score_file_name, sep=";")
            except Exception as error:
                score_data = None
                print(
                    "error in loading Eval Score file "
                    + score_file_name
                    + " ("
                    + str(error)
                    + ")"
                )

        # Initialize from JSON data
        self.__init__(param_eval, score_data, **kwargs)

    def eval_score_df(self, param_eval, score_df, **kwargs):
        """Construction d'une instance `ReactiveEvalScore` à partir du
        dataframe comportant la table pivot."""
        score_data = score_df
        self.__init__(param_eval, score_data, **kwargs)

    def write_report_file(self, file_name):
        """Writes the instance's TSV report to the specified path."""
        with io.open(file_name, "w", encoding="utf-8") as report_file:
            report_file.write("nb individus : \t" + str(self.i_nb_row) + "\n")
            report_file.write(
                "nb targets total : \t" + str(self.nb_target_total) + "\n"
            )
            report_file.write(
                "pourcentage target : \t"
                + str(self.nb_target_total / self.i_nb_row)
                + "\n"
            )
            report_file.write("latency : \t" + str(self.i_latency) + "\n")
            report_file.write(
                "nb jours evaluation : \t" + str(self.i_eval_duration) + "\n"
            )
            aprecision = np.zeros(self.i_bin)
            are_call = np.zeros(self.i_bin)
            nauc = 0
            for itile in range(0, self.i_bin):
                aprecision[itile] = self.list_target_id_sum[itile][
                    self.i_eval_duration - 1
                ] / (((itile + 1) * self.i_nb_row) / self.i_bin)
                if self.nb_target_total > 0:
                    are_call[itile] = (
                        self.list_target_id_sum[itile][
                            self.i_eval_duration - 1
                        ]
                        / self.nb_target_total
                    )
                    nauc = nauc + are_call[itile]
                    report_file.write(
                        "precision/rappel/gain pour TARGET de "
                        + str((itile + 1) / self.i_bin)
                        + " : \t"
                        + str(aprecision[itile])
                        + "\t"
                        + str(are_call[itile])
                        + "\t"
                        + str(
                            aprecision[itile]
                            / (self.nb_target_total / self.i_nb_row)
                        )
                        + "\n"
                    )
            report_file.write(
                "recall mean: \t" + str(nauc / self.i_bin) + "\n"
            )

    def write_report_file_json(self, file_name):
        """Writes the instance's Json report to the specified path."""
        dict_json_file = {}
        dict_json_file["nb individus"] = str(self.i_nb_row)
        dict_json_file["nb targets total"] = str(self.nb_target_total)
        dict_json_file["pourcentage target"] = str(
            self.nb_target_total / self.i_nb_row
        )
        dict_json_file["latency"] = str(self.i_latency)
        dict_json_file["nb jours evaluation"] = str(self.i_eval_duration)
        dict_json_file_precision = {}
        dict_json_file_recall = {}
        dict_json_file_gain = {}

        aprecision = np.zeros(self.i_bin)
        are_call = np.zeros(self.i_bin)
        nauc = 0

        for itile in range(0, self.i_bin):
            aprecision[itile] = self.list_target_id_sum[itile][
                self.i_eval_duration - 1
            ] / (((itile + 1) * self.i_nb_row) / self.i_bin)
            if self.nb_target_total > 0:
                are_call[itile] = (
                    self.list_target_id_sum[itile][self.i_eval_duration - 1]
                    / self.nb_target_total
                )
                nauc = nauc + are_call[itile]

                dict_json_file_precision[str((itile + 1) / self.i_bin)] = str(
                    aprecision[itile]
                )
                dict_json_file_recall[str((itile + 1) / self.i_bin)] = str(
                    are_call[itile]
                )
                dict_json_file_gain[str((itile + 1) / self.i_bin)] = str(
                    aprecision[itile] / (self.nb_target_total / self.i_nb_row)
                )

            dict_json_file["precision"] = dict_json_file_precision
            dict_json_file["rappel"] = dict_json_file_recall
            dict_json_file["gain"] = dict_json_file_gain

            dict_json_file["recall mean"] = str(nauc / self.i_bin)

        with io.open(file_name, "w", encoding="utf-8") as report_file:
            json.dump(dict_json_file, report_file, indent=4)
