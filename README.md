# CDP Spark Iceberg Hive Catalog Information Generator

Used to generate the spark-shell/pyspark command while integrating Spark with Iceberg using Hive Catalog.

## Step1: Download the `spark_iceberg_hive_catalog_info_generator.sh` script.

```sh
wget https://raw.githubusercontent.com/rangareddy/cdp_spark_iceberg_hive_catalog_info_generator/main/spark_iceberg_hive_catalog_info_generator.sh
```

## Step2: Run the `spark_iceberg_hive_catalog_info_generator.sh` script.

```sh
sh spark_iceberg_hive_catalog_info_generator.sh
```

By default `spark_iceberg_hive_catalog_info_generator.sh` script will generate the **spark-shell** command script. If you want to generate the **pyspark** command you need to export the `IS_PYSPARK_SHELL=true`

```sh
export IS_PYSPARK_SHELL=true
sh spark_iceberg_hive_catalog_info_generator.sh
```

