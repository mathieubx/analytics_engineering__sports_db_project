FROM openjdk:19-buster

# Déclaration des variables d’environnement
ENV MB_DB_TYPE=postgres
ENV MB_DB_CONNECTION_URI="jdbc:postgresql://host.docker.internal:5432/metabase?user=postgres&password=metabase-db-password"
ENV MB_PLUGINS_DIR=/home/plugins/

# Ajout du JAR de Metabase et du driver DuckDB
ADD https://downloads.metabase.com/v0.52.4/metabase.jar /home
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12/duckdb.metabase-driver.jar /home/plugins/

# Permissions
RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

# Lancement de Metabase avec l’option --add-opens
CMD ["java", "--add-opens", "java.base/java.nio=ALL-UNNAMED", "-jar", "/home/metabase.jar"]
