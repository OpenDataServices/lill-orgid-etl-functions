import os

AZURE_STORAGE_UPLOAD = True
AZURE_STORAGE_ACCOUNT_NAME = "lillorgiddata"
AZURE_STORAGE_CONTAINER_NAME = "data"
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
