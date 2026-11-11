import pandas as pd
files=[
  {"file": "map_channels"},
  {"file": "map_contract_types"},
  {"file": "bulk_telecom_events"},
  {"file": "map_corporate_accounts"},
  {"file": "map_customer_segments"},
  {"file": "map_devices"},
  {"file": "map_failure_reasons"},
  {"file": "map_governorates"},
  {"file": "map_industries"},
  {"file": "map_loyalty_tiers"},
  {"file": "map_network_types"},
  {"file": "map_offers"},
  {"file": "map_operators"},
  {"file": "map_order_statuses"},
  {"file": "map_payment_methods"},
  {"file": "map_payment_terms"},
  {"file": "map_plans"},
  {"file": "map_products"},
  {"file": "map_sale_types"},
  {"file": "map_sales_channels"},
  {"file": "map_sales_reps"},
  {"file": "map_stores"},
  {"file": "map_service_types"},
  {"file": "map_transaction_statuses"},
  {"file": "map_wallet_transaction_types"}
]

for file in files:

    url = f"https://telestg.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-11-08T17:20:52Z&se=2027-07-01T00:35:52Z&spr=https&sv=2026-02-06&sr=c&sig=JvV5RR6Z0r4Mk1Uu9sNra74%2FeHcNGZVrVPUPIlQOmN4%3D"

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)
    
    # Writing Data To the Bronze Layer
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"telecomservice.onelakedatabase.{file['file']}")
    
