######################################################################################
# Copyright (c) 2023 Orange - All Rights Reserved                                    #
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

    **generate_train_test** : création du dataset
        | Sélection des cibles comprises dans un intervalle temporel à spécifier
        | Tirage aléatoire d'une date pour les non cibles et les cibles hors
            intervalle (tirage parmi les dates de logs si l'intervalle correspond,
            parmi les dates de cible sinon)
        | Constitution des deux datasets train et test

    **fit** : modélisation sur période fixe et/ou sur période mobile

    **predict** : déploiement sur période fixe et/ou sur période mobile
        | Sur une période fixe définie (par exemple 1 mois en marketing)
        | Sur une période mobile : les traitements sont effectués sur une période
            mobile définie en paramètre (par exemple 1, 7 ou 15 jours en marketing)

    **evaluate** : deux métriques d'évaluation
        | précision
        | rappel

    **plot** : tracé, par déciles, des courbes précision et rappel

"""


from kastor.kastor_DB import GenerateDataset
from kastor.kastor_MD import KhiopsClassifier
from kastor.kastor_EV import TemporalEvaluator
import warnings
warnings.simplefilter('ignore')



