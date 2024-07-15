# Metric store snowflake

1) Créer un environnement python 3.10 conda nommé py-snowpark-metric-store
                            conda create -n py-snowpark-metric-store python=3.10 numpy pandas pyarrow jupyter
                            lab tabulate --override-channels -c https://repo.anaconda.com/pkgs/snowflake

2) Activer l’environnement conda: 

                            conda activate py-snowpark-metric-store

3) iNSTALLER LES LIbrairies necessairees:

                            conda install -c https://repo.anaconda.com/pkgs/snowflake snowflake-snowpark-python=1.16.0 snowflake-ml-python=1.5.1 notebook

                            pip install snowflake

                            pip install snowflake-snowpark-python

4) Créer un kernel Jupyter qui représente l’env py-snowpark-metric-store:

                            ipython kernel install --user --name=py-snowpark-metric-store

5) Configurer la connexion snowflake dans le fichier connection.json : 