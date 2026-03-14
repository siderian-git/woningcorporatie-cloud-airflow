import os

EMPIRE_CONNECTION_URL = os.environ.get('EMPIRE_CONNECTION_URL', None)
LAKEHOUSE_URL = os.environ.get('LAKEHOUSE_URL', 'https://core.fuga.cloud:8080')
LAKEHOUSE_S3_BUCKET = os.environ.get('LAKEHOUSE_S3_BUCKET', 'lakehouse')
LAKEHOUSE_S3_ACCESS_KEY = os.environ.get('LAKEHOUSE_S3_ACCESS_KEY', 'c16da65e6c4a489d98d9967dca8cdd0f')
LAKEHOUSE_S3_SECRET_KEY = os.environ.get('LAKEHOUSE_S3_SECRET_KEY', 'af989149f3d142ef8b2d31daac8cf35c')
LAKEHOUSE_HIVE_URI = os.environ.get('LAKEHOUSE_HIVE_URI', 'thrift://hive-metastore.lakehouse.svc.cluster.local:9083')
POSTGRESQL_CONNECTION_URL = os.environ.get('POSTGRESQL_CONNECTION_URL', None)

def initialize_config():
    global EMPIRE_CONNECTION_URL, POSTGRESQL_CONNECTION_URL

    if EMPIRE_CONNECTION_URL is None:
        print("EMPIRE_CONNECTION_URL not set in environment variables, using default values. Please set it to run the test against the actual Empire database.")
        server = "aanswqfq3xduxnq4kcx3gun3c4-m72rt5q7lxeubdwmo7ne6bsn6i.datawarehouse.fabric.microsoft.com"
        database = "Lakehouse_192prd"
        username = "siderian-svc@omniawonen.nl"
        password = "78Pxyvoha2m1OhrFNsvktraCpSP"
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Authentication=ActiveDirectoryPassword;"
        )
        EMPIRE_CONNECTION_URL = f"{conn_str}"

        #EMPIRE_CONNECTION_URL = f"mssql://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server&encrypt=true&trusted_connection=false&trust_server_certificate=true"

    # Settings for on cluster
    if POSTGRESQL_CONNECTION_URL is None:
        server = "lakehouse-database-cluster-r.lakehouse-database.svc.cluster.local"
        database = "lakehouse"
        username = "lakehouse"
        password = "VtRer2ulFmndAZspXUhw"
        port = 5432
        extra_parameters = ""
        # Format: postgresql://user:pass@host:5432/dbname
        POSTGRESQL_CONNECTION_URL = f"postgresql://{username}:{password}@{server}:{port}/{database}"

# # Local settings
# if POSTGRESQL_CONNECTION_URL is None:
#     server = "localhost"
#     database = "lakehouse"
#     username = "lakehouse"
#     password = "VtRer2ulFmndAZspXUhw"
#     port = 9432
#     extra_parameters = "?sslmode=disable"
#     # Format: postgresql://user:pass@host:5432/dbname
#     POSTGRESQL_CONNECTION_URL = f"postgresql://{username}:{password}@{server}:{port}/{database}{extra_parameters}"
