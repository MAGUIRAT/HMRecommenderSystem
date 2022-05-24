set -e

sleep 5

# shellcheck disable=SC2046
export $(grep -v '^#' ../../.env | xargs)

cd "${AIRFLOW_HOME}"

airflow db check
mysql --user=root --password="${MYSQL_ROOT_PASSWORD}" --protocol=tcp --host=airflow_db --execute="SET GLOBAL explicit_defaults_for_timestamp=ON;"
airflow db init
airflow users create --role="${AIRFLOW_UI_USERROLE}" --username="${AIRFLOW_UI_USERNAME}" --password="${AIRFLOW_UI_PASSWORD}" --firstname=airflow_user --lastname=airflow_user --email=airflow@airflow.com
airflow scheduler&
airflow webserver
