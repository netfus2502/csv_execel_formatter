import pandas as pd
import sqlite3
import os

def process_and_store_sales_data(base_path):
    files = {
        "A": os.path.join(base_path, "sales_a.csv"),
        "B": os.path.join(base_path, "sales_b.csv"),
        "C": os.path.join(base_path, "sales_c.csv")
    }

    dfs = []

    for key, path in files.items():
        if not os.path.exists(path):
            print(f"⚠️ ファイルが見つかりません: {path}")
            continue

        df = pd.read_csv(path)

        if key == "A":
            df.columns = ["date", "client", "item", "amount"]
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        elif key == "B":
            df.columns = ["date", "client", "item", "amount"]
            df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d")
        elif key == "C":
            df.columns = ["date", "client", "item", "amount"]
            df["date"] = df["date"].apply(lambda x: pd.to_datetime(
                x.replace("年", "-").replace("月", "-").replace("日", "")
            ))

        df["amount"] = df["amount"].astype(int)
        dfs.append(df)

    if not dfs:
        print("⚠️ 有効なデータがありません。")
        return

    df_all = pd.concat(dfs, ignore_index=True)
    df_all["month"] = df_all["date"].dt.to_period("M").astype(str)

    conn = sqlite3.connect(os.path.join(base_path, "sales.db"))
    df_all.to_sql("sales_data", conn, if_exists="replace", index=False)
    conn.close()

    print("✅ sales_data に登録完了")
    return df_all

# 実行トリガー
if __name__ == "__main__":
    base_path = "C:/Users/USER/Desktop/lesson/テスト"
    df_all = process_and_store_sales_data(base_path)
    print(df_all.head())