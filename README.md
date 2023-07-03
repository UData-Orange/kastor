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

Regardez les exemples disponibles dans `tutorial` dans cet répo où téléchargez-le dans [le
wiki](https://gitlab.tech.orange/udata/scoring4tsdb/-/wikis/home).

Le Tutoriel `tuto_librairie_churn_xdsl.py` permet d’exécuter la librairie sur un jeu de données de
churn xdsl, pour comparer les performances entre le modèle sur une période d’un mois actuellement
utilisé et les modèles sur les périodes de 1, 7 ou 15 jours.

## Structure du package

Cette librairie python permet de dérouler la chaine de production de scores de bout en bout. Les
traitements Khiops sont automatisés via pyKhiops, allant de la production du modèle à la
restitution de scores par période prédéfinies ainsi qu'à leur évaluation.

Cette package contient les suivantes sous-modules :
  - `creation_dataset` (création du dataset) :
    - sélection des cibles comprises dans un intervalle à spécifier constitution des deux datasets
      train et test

  - `etude_periode_fixe` (modélisation, déploiement et évaluation sur période fixe) :
    - les traitements sont effectués sur une période fixe définie (par exemple 1 mois en marketing)

  - `etude_periode_mobile` (modélisation, déploiement et évaluation sur période mobile):
    - les traitements sont effectués sur une période mobile définie en paramètre (par exemple 1,
      7 ou 15 jours en marketing)
