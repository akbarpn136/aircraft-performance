from dask import (dataframe as dd, array as da)


def load_data(projectname):
    """Fungsi untuk baca dan olah data"""

    kolom = ["RUN", "V0", "Q0", "ALPHA", "BETA", "CL", "CD", "CM25", "CY", "CYAW", "CROLL"]

    df = dd.read_csv(f"data/{projectname}/run*.csv", sep="\t", header=None, names=kolom)
    df = df[["RUN", "ALPHA", "CL", "CD", "CM25"]]

    return df


def column_builder(df, start=20, stop=40, step=5):
    """Fungsi ini digunakan untuk membangun tabel dengan kolom dinamis dalam bentuk dataframe"""

    import numpy as np

    # Membuat rentang array yang diperlukan
    variance = np.arange(start, stop, step)

    # Pembuatan kolom tabel dinamik pertama
    columns = {}
    coef = ["CLCG", "CDCG"]
    name = [f'{c}_{var}' for c in coef for var in variance]  # membuat kombinasi nama dan % CG kedalam list

    for cols in name:
        cg_position = int(cols[-2:]) / 100

        if "CL" in cols:
            cl = cols
            # Lakukan perhitungan untuk variasi CG sesuai perhitungan excel bagian CL
            columns[cl] = lambda row: row["CM25"] + (cg_position - 0.25) * df["CL"]

        else:
            cd = cols
            # Lakukan perhitungan untuk variasi CG sesuai perhitungan excel bagian CD
            columns[cd] = lambda row: row["CM25"] + (cg_position - 0.25) * df["CD"]

    # Tambahkan kolom dinamik ke dataframe
    df = df.assign(**columns)

    # Pembuatan kolom tabel dinamik kedua (kondisi trim)
    columns_trim = {}
    coef_trim = ["TRIM_CLCG", "TRIM_CDCG"]
    name_trim = [f'{c}_{var}' for c in coef_trim for var in variance]  # membuat kombinasi nama dan % CG kedalam list
    for cols in name_trim:
        if "CL" in cols:
            # Lakukan perhitungan untuk kondisi TRIM CG sesuai perhitungan excel bagian CL
            cl = cols.replace("TRIM_", "")
            columns_trim[cols] = lambda row: row["CL"].diff(-1).fillna(0) / row[cl].diff(-1).fillna(0) * -1 * \
                                             row[cl] + row["CL"]

        elif "CD" in cols:
            # Lakukan perhitungan untuk kondisi TRIM CG sesuai perhitungan excel bagian CD
            cd = cols.replace("TRIM_", "")
            columns_trim[cols] = lambda row: row["CD"].diff(-1).fillna(0) / row[cd].diff(-1).fillna(0) * -1 \
                                             * row[cd] + row["CD"]

    df = df.assign(**columns_trim)

    return df.drop(["ALPHA", "CL", "CD", "CM25"], axis=1)


def calc_trim(df):
    """Fungsi ini digunakan untuk menghitung koefisien kondisi trim"""

    unique_columns = list(set([col.replace("TRIM_", "") for col in df.columns.values if col != "RUN"]))

    def __get_trim_data(data, column_names):
        """Fungsi ini digunakan untuk mendeteksi perubahan tanda positif dan negatif dalam kolom yang didefinisikan dan
        dibuat private function"""

        trim = []

        for col in column_names:
            # mencari pola tanda positif negatif dalam kolom
            sign = data[col].map(da.sign)

            # mencari indeks dengan tanda positif dalam kolom sebelumnya
            diff_positive = sign.diff(periods=-1).fillna(0)

            # baca dataframe berdasarkan indeks positif tadi
            df_positive = data.loc[diff_positive[diff_positive != 0].index]

            # pilih kolom yang ditampilkan dan tampilkan satu
            df_positive = df_positive[f"TRIM_{col}"].head(1).values

            trim.append(df_positive)

        return da.concatenate(trim, axis=0).compute()

    df = df.map_partitions(__get_trim_data, unique_columns)

    return df.compute()


def process():
    # Memuat keseluruhan data windtunnel berupa RUN numbers berdasarkan nama pesawat
    df = load_data("male")

    # Membuat kolom dinamis sesuai variasi CG yang diinginkan misalkan 20 untuk 20% dan seterusnya
    df = column_builder(df, 20, 30)
    df.to_csv("result*.csv")


if __name__ == "__main__":
    process()
