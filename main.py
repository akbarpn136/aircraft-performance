from dask import (dataframe as dd, array as da)


def load_data(projectname):
    """Fungsi untuk baca dan olah data"""

    start = 32
    kolom = ["V0", "Q0", "ALPHA", "BETA", "CL", "CD", "CM25", "CY", "CYAW", "CROLL"]

    df = dd.read_fwf(f"data/{projectname}/print*", header=None, skiprows=start, skipfooter=2, include_path_column=True,
                     names=kolom)
    df = df[["path", "ALPHA", "CL", "CD", "CM25"]]
    df["RUN"] = df["path"].apply(lambda x: x.split("/")[-1], meta=("path", "string"))
    df = df.drop(["path"], axis=1)

    return df


def column_builder(df, start=20, stop=40, step=5):
    """Fungsi ini digunakan untuk membangun tabel dengan kolom dinamis dalam bentuk dataframe"""

    import numpy as np

    # Membuat rentang array yang diperlukan
    variance = np.arange(start, stop, step)
    coef = ["CLCG", "CDCG", "TRIM_CLCG", "TRIM_CDCG"]
    column_names = [f'{c}_{var}' for c in coef for var in variance]  # membuat kombinasi nama dan % CG kedalam list

    for col in column_names:
        cg_position = int(col[-2:]) / 100
        coef_selection = col[:2]

        if "TRIM_" in col:
            cg_name = col.replace("TRIM_", "")
            df[col] = df[cg_name[:2]].diff(-1).fillna(0) / df[cg_name].diff(-1).fillna(0) * -1 * df[cg_name] + df[
                cg_name[:2]]
        else:
            df[col] = df["CM25"] + (cg_position - 0.25) * df[coef_selection]

    return df.drop(["ALPHA", "CL", "CD", "CM25"], axis=1)


def calc_trim(df):
    """Fungsi ini digunakan untuk menghitung koefisien kondisi trim"""

    unique_columns = list(set([col.replace("TRIM_", "") for col in df.columns.values if col != "RUN"]))

    def __get_trim_data(data, column_names):
        """Fungsi ini digunakan untuk mendeteksi perubahan tanda positif dan negatif dalam kolom yang didefinisikan dan
        dibuat private function"""

        trim = {}

        for col in column_names:
            # mencari pola tanda positif negatif dalam kolom
            sign = data[col].map(da.sign)

            # mencari indeks dengan tanda positif dalam kolom sebelumnya
            diff_positive = sign.diff(periods=-1).fillna(0)

            # baca dataframe berdasarkan indeks positif tadi
            df_positive = data.loc[diff_positive[diff_positive != 0].index]

            # pilih kolom yang ditampilkan dan tampilkan satu
            df_positive = df_positive[["RUN", f"TRIM_{col}"]].head(1)

            if df_positive.size > 0:
                trim["RUN"] = df_positive["RUN"].values[0]
                trim[f"TRIM_{col}"] = df_positive[f"TRIM_{col}"].values[0]

        return trim

    df = df.map_partitions(__get_trim_data, unique_columns)

    return df.compute()


def calc_performance(df, mass, area, rho=1.225):
    import pandas as pd

    df = pd.DataFrame.from_records(df).dropna()
    df["RUN"] = df["RUN"].apply(lambda x: x.split(".")[0].replace("print", "RUN"))
    df = df.set_index("RUN")
    numbers = list(set([int(num.split("_")[-1]) for num in df.columns.values]))

    for num in numbers:
        df[f"CLCD_{num}"] = df[f"TRIM_CLCG_{num}"] / df[f"TRIM_CDCG_{num}"]
        df[f"AIRSPEED_{num}"] = (2 * mass * 10 / (rho * area * df[f"TRIM_CLCG_{num}"])) ** 0.5
        df[f"THRUST_{num}"] = 0.5 * rho * area * df[f"TRIM_CDCG_{num}"] * (df[f"AIRSPEED_{num}"] ** 2)
        df[f"POWER_{num}"] = df[f"AIRSPEED_{num}"] * df[f"THRUST_{num}"]

    return df


def process():
    # Memuat keseluruhan data windtunnel berupa RUN numbers berdasarkan nama pesawat
    df = load_data("alap")

    # Membuat kolom dinamis sesuai variasi CG yang diinginkan misalkan 20 untuk 20% dan seterusnya
    df = column_builder(df, 20, 30)

    try:
        trim = calc_trim(df).tolist()

        mass = 1300
        area = 12.8
        performance = calc_performance(trim, mass, area)

        performance.to_json("res.json", orient="columns")

    except ValueError:
        print("Ada kesalahan saat Parsing data Windtunnel, barangkali ada tabel ganda didalam data print windtunnel.")


if __name__ == "__main__":
    process()
