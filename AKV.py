# Databricks notebook source
# 从Key Vault中检索密钥
blob_storage_key = dbutils.secrets.get(scope="my-key-vault-scope", key="test1s")
print(blob_storage_key)


# COMMAND ----------

# 从Key Vault中检索密钥
storage_account_name = "satest101601"
blob_storage_key = dbutils.secrets.get(scope="my-key-vault-scope", key="test1s")

# 使用机密来配置Blob存储连接
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    blob_storage_key
)

# 使用机密来操作Blob存储
container_name = "contain1016"
file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/a.txt"

df = spark.read.format("csv").option("header", "true").load(file_path)
df.show()


# COMMAND ----------


