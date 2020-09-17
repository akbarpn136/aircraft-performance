from dask import (dataframe as dd, array as da)


def load_data(projectname):
    """Fungsi untuk baca dan olah data"""

    kolom = ["RUN", "V0", "Q0", "ALPHA", "BETA", "CL", "CD", "CM25", "CY", "CYAW", "CROLL"]

    df = dd.read_csv(f"data/{projectname}/run*.csv", sep="\t", header=None, names=kolom)
    df = df[["RUN", "ALPHA", "CL", "CD", "CM25"]]

    # CLCG20 CLCG25 CLCG30 CLCG35 merupakan variabel untuk variasi CM terhadap CG masing-masing 20%, 25%, 30% dan 35%
    # dalam perhitungan CL trim
    df["CLCG20"] = df["CM25"] + (0.20 - 0.25) * df["CL"]
    df["CLCG20_TRIM"] = df["CL"].diff(-1).fillna(0) / df["CLCG20"].diff(-1).fillna(0) * -1 * df["CLCG20"] + df["CL"]

    df["CLCG25"] = df["CM25"] + (0.25 - 0.25) * df["CL"]
    df["CLCG25_TRIM"] = df["CL"].diff(-1).fillna(0) / df["CLCG25"].diff(-1).fillna(0) * -1 * df["CLCG25"] + df["CL"]

    df["CLCG30"] = df["CM25"] + (0.30 - 0.25) * df["CL"]
    df["CLCG30_TRIM"] = df["CL"].diff(-1).fillna(0) / df["CLCG30"].diff(-1).fillna(0) * -1 * df["CLCG30"] + df["CL"]

    df["CLCG35"] = df["CM25"] + (0.35 - 0.25) * df["CL"]
    df["CLCG35_TRIM"] = df["CL"].diff(-1).fillna(0) / df["CLCG35"].diff(-1).fillna(0) * -1 * df["CLCG35"] + df["CL"]

    # CDCG20 CDCG25 CDCG30 CDCG35 merupakan variabel untuk variasi CM terhadap CG masing-masing 20%, 25%, 30% dan 35%
    # dalam perhitungan CD trim
    df["CDCG20"] = df["CM25"] + (0.20 - 0.25) * df["CD"]
    df["CDCG20_TRIM"] = df["CD"].diff(-1).fillna(0) / df["CDCG20"].diff(-1).fillna(0) * -1 * df["CDCG20"] + df["CD"]

    df["CDCG25"] = df["CM25"] + (0.25 - 0.25) * df["CD"]
    df["CDCG25_TRIM"] = df["CD"].diff(-1).fillna(0) / df["CDCG25"].diff(-1).fillna(0) * -1 * df["CDCG25"] + df["CD"]

    df["CDCG30"] = df["CM25"] + (0.30 - 0.25) * df["CD"]
    df["CDCG30_TRIM"] = df["CD"].diff(-1).fillna(0) / df["CDCG30"].diff(-1).fillna(0) * -1 * df["CDCG30"] + df["CD"]

    df["CDCG35"] = df["CM25"] + (0.35 - 0.25) * df["CD"]
    df["CDCG35_TRIM"] = df["CD"].diff(-1).fillna(0) / df["CDCG35"].diff(-1).fillna(0) * -1 * df["CDCG35"] + df["CD"]

    return df.drop(["ALPHA", "CL", "CD", "CM25"], axis=1)


def __get_trim_data(df, column_name):
    """Fungsi ini digunakan untuk mendeteksi perubahan tanda positif dan negatif dalam kolom yang didefinisikan dan
    dibuat private function"""

    sign = df[column_name].map(da.sign)  # mencari pola tanda positif negatif dalam kolom
    diff_positive = sign.diff(periods=-1).fillna(0)  # mencari indeks dengan tanda positif dalam kolom sebelumnya
    df_positive = df.loc[diff_positive[diff_positive != 0].index]  # baca dataframe berdasarkan indeks positif tadi
    df_positive = df_positive[["RUN", f"{column_name}_TRIM"]].head(1)  # pilih kolom yang ditampilkan dan tampilkan satu

    return df_positive


def calc_trim(df):
    """Fungsi ini digunakan untuk menghitung koefisien kondisi trim"""

    # Lakukan perhitungan CL trim per data RUN
    clcg20_trim = df.map_partitions(__get_trim_data, "CLCG20")
    clcg25_trim = df.map_partitions(__get_trim_data, "CLCG25")
    clcg30_trim = df.map_partitions(__get_trim_data, "CLCG30")
    clcg35_trim = df.map_partitions(__get_trim_data, "CLCG35")

    # Lakukan perhitungan CD trim per data RUN
    cdcg20_trim = df.map_partitions(__get_trim_data, "CDCG20")
    cdcg25_trim = df.map_partitions(__get_trim_data, "CDCG25")
    cdcg30_trim = df.map_partitions(__get_trim_data, "CDCG30")
    cdcg35_trim = df.map_partitions(__get_trim_data, "CDCG35")

    # Gabung banyak dataframe melalui merge
    trim = clcg20_trim.merge(clcg25_trim).merge(clcg30_trim).merge(clcg35_trim).merge(cdcg20_trim).merge(
        cdcg25_trim).merge(cdcg30_trim).merge(cdcg35_trim)

    return trim


def process():
    # Memuat keseluruhan data windtunnel berupa RUN numbers berdasarkan nama pesawat
    df = load_data("male")

    # Tampilkan hasil perhitungan ke terminal
    print(calc_trim(df).compute())


if __name__ == "__main__":
    process()
