#!/bin/bash

#################################################################################################################################
#                                                                                                                               #
#    Name               :   spark_iceberg_hive_catalog_info_generator.sh                                                        #
#    Purpose            :   Used to generate the Spark Iceberg command using Hive Catalog                                       #
#    Author             :   Ranga Reddy                                                                                         #
#    Created Date       :   12-Dec-2023                                                                                         #
#    Version            :   v1.0                                                                                                #
#                                                                                                                               #
#################################################################################################################################

# log_info() is used to log the message based on logging level. By default logging level will be INFO.
log_info() {
    if [[ "$#" -gt 0 ]]; then
        current_date_time=$(date +'%m/%d/%Y %T')
        info_level="INFO"
        info_message=${1}
        if [[ "$#" -gt 1 ]]; then
            info_level=${1}
            info_message=${2}
        fi
        Pattern="${current_date_time} ${info_level} : ${info_message}"
        echo "${Pattern}"
    fi
}

echo ""

# Set default values for environment variables
export IS_SPARK_SHELL="${IS_SPARK_SHELL:-true}"
export IS_PYSPARK_SHELL="${IS_PYSPARK_SHELL:-false}"
export SPARK_SHELL="spark3-shell"
export PYSPARK_SHELL="pyspark3"

# Catalog name and database name
export ICEBERG_CATALOG_NAME=${ICEBERG_CATALOG_NAME:-"iceberg_catalog"}
export DEFAULT_CATALOG_NAME=${DEFAULT_CATALOG_NAME:-"spark_catalog"}
export DEFAULT_DATABASE_NAME=${DEFAULT_DATABASE_NAME:-"default"}
export DEFAULT_TABLE_NAME=${DEFAULT_TABLE_NAME:-"iceberg_table"}

# Iceberg library directory
export SPARK_ICEBERG_LIB_DIR="/opt/cloudera/parcels/SPARK3/lib/iceberg"

# Validate script configuration
validate_script_conf() {
    ERROR_MSG=""

    # Check if Iceberg library directory exists
    if [ ! -d "$SPARK_ICEBERG_LIB_DIR" ]; then
        log_info "error" "Spark Iceberg library directory '$SPARK_ICEBERG_LIB_DIR' not found. Please check if you have installed Spark version 3.3.2 or above."
        exit 1
    fi

    # Locate iceberg-spark-runtime library
    export ICEBERG_SPARK_RUNTIME_JAR=$(find -L /opt/cloudera/parcels/SPARK3/lib/iceberg -name 'iceberg-spark*')

    # Check if iceberg-spark-runtime library is found
    if [ -z "$ICEBERG_SPARK_RUNTIME_JAR" ]; then
        ERROR_MSG="The required 'iceberg-spark-runtime' library was not found in the '$SPARK_ICEBERG_LIB_DIR' directory."
    fi

    # Determine HIVE_METASTORE_URI
    if [ -z $HIVE_METASTORE_URI ]; then
        export HIVE_SITE_XML_FILE_PATH="/etc/hive/conf/hive-site.xml"
        export HIVE_SITE_XML_FILE=$(ls ${HIVE_SITE_XML_FILE_PATH})
    fi

    # Check for hive-site.xml file
    if [ -z $HIVE_METASTORE_URI ] && [ ! -f "$HIVE_SITE_XML_FILE" ]; then
        ERROR_MSG="<hive-site.xml> file does not exist on this host or the current user <$(whoami)> does not have access to ${HIVE_SITE_XML_FILE_PATH} file."
    fi

    # Exit if errors are found
    if [ ! -z "$ERROR_MSG" ]; then
        log "ERROR" ${ERROR_MSG}
        exit 1
    fi

    # Extract HIVE_METASTORE_URI from hive-site.xml (if needed)
    if [ -z $HIVE_METASTORE_URI ]; then
        export HIVE_METASTORE_URI=$(grep "thrift.*9083" "$HIVE_SITE_XML_FILE" | awk -F"<|>" '{print $3}')
    fi
}

validate_script_conf

# Generating spark-shell script and code
generate_spark_shell_script() {
    echo ""
    log_info "Launch the $SPARK_SHELL by coping the following command"
    echo "======================================================"
    echo "$SPARK_SHELL --master yarn --jars $ICEBERG_SPARK_RUNTIME_JAR --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.$ICEBERG_CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.$ICEBERG_CATALOG_NAME.type=hive --conf spark.sql.catalog.$ICEBERG_CATALOG_NAME.uri=\"$HIVE_METASTORE_URI\""
    echo ""
    echo "------------------------------------------------------"
    echo "After launching the $SPARK_SHELL run the following code"
    echo "------------------------------------------------------"
    echo "spark.sql(\"CREATE DATABASE IF NOT EXISTS ${ICEBERG_CATALOG_NAME}.${DEFAULT_DATABASE_NAME}\")"
    echo "spark.sql(\"USE ${ICEBERG_CATALOG_NAME}.${DEFAULT_DATABASE_NAME}\")"
    echo "spark.sql(\"CREATE TABLE IF NOT EXISTS $DEFAULT_TABLE_NAME (id int, name string) USING iceberg\")"
    echo "spark.sql(\"INSERT INTO $DEFAULT_TABLE_NAME VALUES (1, 'Ranga'), (2, 'Nishanth')\")"
    echo "spark.sql(\"SELECT * FROM $DEFAULT_TABLE_NAME\").show()"
    echo ""
}

# Generating Pyspark script and code
generate_pyspark_shell_script() {
    echo ""
    log_info "Launch the $PYSPARK_SHELL by coping the following command"
    echo "======================================================"
    echo "$PYSPARK_SHELL --master yarn --jars $ICEBERG_SPARK_RUNTIME_JAR --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.$ICEBERG_CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.$ICEBERG_CATALOG_NAME.type=hive --conf spark.sql.catalog.$ICEBERG_CATALOG_NAME.uri=\"$HIVE_METASTORE_URI\""
    echo "======================================================"
    echo ""
    echo "------------------------------------------------------"
    echo "After launching the $PYSPARK_SHELL run the following code"
    echo "------------------------------------------------------"
    echo "spark.sql(\"CREATE DATABASE IF NOT EXISTS ${ICEBERG_CATALOG_NAME}.${DEFAULT_DATABASE_NAME}\")"
    echo "spark.sql(\"USE ${ICEBERG_CATALOG_NAME}.${DEFAULT_DATABASE_NAME}\")"
    echo "spark.sql(\"CREATE TABLE IF NOT EXISTS $DEFAULT_TABLE_NAME (id int, name string) USING iceberg\")"
    echo "spark.sql(\"INSERT INTO $DEFAULT_TABLE_NAME VALUES (1, 'Ranga'), (2, 'Nishanth')\")"
    echo "spark.sql(\"SELECT * FROM $DEFAULT_TABLE_NAME\").show()"
    echo ""
}

if [ ${IS_SPARK_SHELL} ]; then
    generate_spark_shell_script
fi

if [ ${IS_PYSPARK_SHELL} == true ]; then
    generate_pyspark_shell_script
fi
