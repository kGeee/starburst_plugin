"# starburst_plugin" 

## Installation

Clone the git repository via 

1. `git clone https://github.com/kGeee/starburst_plugin.git`
2. Move the starburst_plugin folder to your airflow plugins directory `~AIRFLOW_HOME/plugins`
3. *[Optional]* Move the included `example_dags` directory to your `~AIRFLOW_HOME/dags`


## Using the connection
The starburst connection must be configured in airflow connections before running the operator. The default connection string is **starburst_default**.

- Connection Id : starburst_default
- Connection Type : Starburst
- Host : Host Address
- Port : Host Port `38080`
- Login : User login `trino`
- Password : User password
