import polars as pl
from src.api.azure_blob import BlobCon

blob_con = BlobCon()

collect = [
    pl.read_parquet(
        blob_con.read_data_from_blob(blob.get("name"), as_text=False)
        )
    for blob in blob_con.list_blob("wot_data")
]

tank_df = (
    pl.concat(collect)
    .filter(pl.col("battles") > 100)
    .with_columns(
        (pl.col("wins") / pl.col("battles") * 100).alias("winrate"),
        (pl.col("damage_dealt") / pl.col("battles")).alias("avg_damage")
    )
    .with_columns(
        pl.col("winrate")
        .rank("dense", descending=True)
        .over(partition_by="account_id")
        .alias("tank_rank_per_player")
    )
    .sort(["account_id", "tank_rank_per_player"])
)

print(tank_df)

agg_tank_df = (
    tank_df
    .filter(pl.col("tank_rank_per_player") <= 3)
    .group_by("account_id")
    .agg(
            (pl.col("wins").sum()/pl.col("battles").sum() * 100).alias("avg_top3_winrate")
        )
    )

    
print(agg_tank_df)