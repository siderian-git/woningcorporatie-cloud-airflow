import os

EMPIRE_CONNECTION_URL = os.environ.get('EMPIRE_CONNECTION_URL', None)
LAKEHOUSE_URL = os.environ.get('LAKEHOUSE_URL', 'https://core.fuga.cloud:8080')
LAKEHOUSE_S3_BUCKET = os.environ.get('LAKEHOUSE_S3_BUCKET', 'lakehouse')
LAKEHOUSE_S3_ACCESS_KEY = os.environ.get('LAKEHOUSE_S3_ACCESS_KEY', None)
LAKEHOUSE_S3_SECRET_KEY = os.environ.get('LAKEHOUSE_S3_SECRET_KEY', None)
LAKEHOUSE_HIVE_URI = os.environ.get('LAKEHOUSE_HIVE_URI', 'thrift://hive-metastore.hive.svc.cluster.local:9083')
LAKEHOUSE_POSTGRESQL_CONNECTION_URL = os.environ.get('POSTGRESQL_CONNECTION_URL', None)

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



