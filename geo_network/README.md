## GEO Network
### 1. Build the network
* _geolib_ should be installed for the Python your Spark uses (The PySpark3 contains _geolib_ on 219)

`nohup /opt/spark3/bin/spark-submit build_graph.py --print_group_stats > log_build_graph &`

### 2. Run connected components on the network
* the jar file of _graphframes_ should be added to the _PYTHONPATH_ environment variable (It has already been added on 219)

`nohup spark-submit --jars graphframes-0.8.0-spark2.4-s_2.11.jar conn_comp.py --print_comp_stats > log_conn_comp &`

* if not added, then

`nohup spark-submit --py-files graphframes-0.8.0-spark2.4-s_2.11.jar --jars graphframes-0.8.0-spark2.4-s_2.11.jar conn_comp.py --print_comp_stats > log_conn_comp &`
