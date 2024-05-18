### Airflow Architecture
<img width="943" alt="image002" src="https://github.com/Pramita0410/Apache-Airflow-Demo/assets/114774760/f7fc1e71-5c1c-42fa-a8d9-37a75a4c7371">


### How to install and run Airflow on ubuntu

* Install conda https://docs.anaconda.com/free/anaconda/install/linux/
* Create a new conda env
    * Conda create -n "airflow" python=3.8 ipython
* Activate the environment
    * conda activate airflow
* Setup Home env variable
    * export AIRFLOW_HOME=/home/<YOUR_USERNAME>/Desktop/airflow
* Initialize Airflow DB for the first time only 
    * airflow db init
* Run the Airflow webserver
    * airflow webserver
* Run the Airflow scheduler
    * airflow scheduler


To install providers
* MySQL provider
    * pip install apache-airflow-providers-mysql
* AWS provider
    * pip install apache-airflow-providers-amazon
* Setup SMTP server for airflow using Gmail
    * https://stackoverflow.com/a/51837049
