# Scoring4TSDB

Scoring4TSDB est une librairie pour automatiser la chaine de production de scores des séries
temporelles.

## Installation

Téléchargez le package `scoring4tsdb-<VERSION>.tar.gz` depuis [le
wiki](https://gitlab.tech.orange/udata/scoring4tsdb/-/packages). Ensuite exécutez:

```bash
pip install scoring4tsdb-<VERSION>.tar.gz
```

### Pré-requis
- [Khiops](https://www.khiops.com)
- [pyKhiops](https://www.khiops.com/html/pykhiops-doc)

## Utilisation

Regardez les exemples disponibles dans `tutorial` dans ce repo où téléchargez-le dans [le
wiki](https://gitlab.tech.orange/udata/scoring4tsdb/-/wikis/home).

Le Tutoriel `tuto_librairie_churn_xdsl.py` permet d’exécuter la librairie sur un jeu de données de
churn xdsl, pour comparer les performances entre le modèle sur une période d’un mois actuellement
utilisé et les modèles sur les périodes de 1, 7 ou 15 jours.

## Structure du package

Cette librairie python propose une automatisation de la chaine de production de scores pour des données temporelles.
Elle permet de dérouler la chaine de production de scores de bout en bout.
Les traitements Khiops sont automatisés via pykhiops, allant de la production du modèle à la restitution de scores par période predéfinies ainsi qu’à leur évaluation.
Les modules définis sont les suivants :

Ce package contient les sous-modules suivants :
  - `generate_train_test` création du dataset
    - sélection des cibles comprises dans un intervalle temporel à spécifier
    - tirage aléatoire d’une date pour les non cibles et les cibles hors intervalle (tirage parmi les dates de logs si l’intervalle correspond, parmi les dates de cible sinon)
    - constitution des deux datasets train et test

  - `fit` modélisation sur période fixe et/ou sur période mobile

  - `predict` déploiement sur période fixe et/ou sur période mobile
    - sur une période fixe définie (par exemple 1 mois en marketing)
    - sur une période mobile : les traitements sont effectués sur une période mobile définie en paramètre (par exemple 1, 7 ou 15 jours en marketing)

  - `evaluate` deux métriques d'évaluation
    - précision
    - rappel
    
  - `plot` tracé, par décile, des courbes précision et rappel


# FE4MT
Ce projet vise à construire un système permettant d'explorer les données multitables afin d'en extraire les paramètres pertinents pour la classification. Les paramètres pris en compte ici sont les variables natives secondaires (variables présentes dans les tables secondaires) et les primitives de construction (règles mathématiques).

Ce système s'est dans un premier temps concentré sur la création d'une mesure d'importance des variables et des primitives vis-à-vis de la variable cible. L'objectif final sera ainsi de réduire l'espace des primitives et des variables pour sélectionner uniquement les éléments pertinents.

## Méthode employée
La méthode d'estimation d'importance employée est une méthode univariée avec discrétisation de la variable Count pour limiter l'effet du bruit sur les tables secondaires. Les explications et les différentes expérimentations ayant mené au choix de cette méthode sont présentées dans le rapport de stage [Rapport_Stage_Lou-Anne_Quellet](Rapport_Stage_Lou-Anne_Quellet.pdf).

##Organisation

Les classes :
  - kastor/class_UnivariateMultitableAnalysis.py
  - kastor/class_VariableSelectionStatistics.py

Les données :
  - notebook/data/Accident/ : jeu de données Accident (possède son propre README)

