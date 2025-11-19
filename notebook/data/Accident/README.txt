# "Accidents" Dataset
France traffic accident data from the year 2018.

## Schema
This dataset has four tables `Accident`, `Vehicle`, `Place` and `User` organized in the following
relational schema.

```
Accident
|
| -- 1:n -- Vehicle
|             |
|             |-- 1:n -- User
|
| -- 1:1 -- Place
```

Each accident has associated one or more vehicles and one unique place. The vehicles involved in an
accident have in turn associated one or more road users (passengers and pedestrians).

The fields of each table are self-explanatory (see `Accidents.kdic`), and so are their values. The
target in the `Accident` table is the constructed variable `Gravity` which is set to `Lethal` if
there was at least one casualty in the accident (see the `train.py` Python script for an example on
how to train a classifier with pyKhiops).

## Preprocessing & Origin
See the PDFs, README file and scripts in the `raw` directory for more details about the
preprocessing of and origin of this dataset (PDFs are in French).
