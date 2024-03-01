Installation Guide 
------------------
pip install apache-airflow
conda install -c conda-forge pyspark
pip install apache-airflow-providers-apache-spark

Delta Lake : https://towardsdatascience.com/hands-on-introduction-to-delta-lake-with-py-spark-b39460a4b1ae

Execution
------------------

create user (one time) : 
airflow users create --email <email> --firstname <fname> --lastname <lname> --password admin --role Admin --username admin

	Initiate Airflow Server
	-----------------------
	
	airflow db init
	airflow scheduler
	airflow webserver -p 8080
