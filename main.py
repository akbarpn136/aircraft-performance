from dask import (dataframe as dd, array as da)


def load_data(filename):
    """Fungsi untuk baca dan olah data"""

    kolom = ["V0", "Q0", "ALPHA", "BETA", "CL", "CD", "CM25", "CY", "CYAW", "CROLL"]

    df = dd.read_csv(f"data/{filename}", sep="\t", header=None, names=kolom)
    df = df[["ALPHA", "CL", "CD", "CM25"]]

    # CLCG20 CLCG25 CLCG30 CLCG35 merupakan variabel untuk variasi CM terhadap CG masing-masing 20%, 25%, 30% dan 35%
    # dalam perhitungan CL trim
    df["CLCG20"] = df["CM25"] + (0.20 - 0.25) * df["CL"]
    df["CLCG25"] = df["CM25"] + (0.25 - 0.25) * df["CL"]
    df["CLCG30"] = df["CM25"] + (0.30 - 0.25) * df["CL"]
    df["CLCG35"] = df["CM25"] + (0.35 - 0.25) * df["CL"]

    # CDCG20 CDCG25 CDCG30 CDCG35 merupakan variabel untuk variasi CM terhadap CG masing-masing 20%, 25%, 30% dan 35%
    # dalam perhitungan CD trim
    df["CDCG20"] = df["CM25"] + (0.20 - 0.25) * df["CD"]
    df["CDCG25"] = df["CM25"] + (0.25 - 0.25) * df["CD"]
    df["CDCG30"] = df["CM25"] + (0.30 - 0.25) * df["CD"]
    df["CDCG35"] = df["CM25"] + (0.35 - 0.25) * df["CD"]

    return df


def _find_trim_files(row):
    """
    Fungsi untuk mencari index saat terjadi kondisi trim per run pengujian. Fungsi ini juga bersifat lazy sehingga untuk
    mendapatkan hasil perhitungan harus memanggil perintah compute()
    """

    # Index dari dataframe df (CL trim) dimana CM bernilai negatif disimpan kedalam Array
    trim_idx_cl_20 = row[row["CLCG20"] < 0].head(1).index.values
    trim_idx_cl_25 = row[row["CLCG25"] < 0].head(1).index.values
    trim_idx_cl_30 = row[row["CLCG30"] < 0].head(1).index.values
    trim_idx_cl_35 = row[row["CLCG35"] < 0].head(1).index.values

    # Index dari dataframe df (CD trim) dimana CM bernilai negatif disimpan kedalam Array
    trim_idx_cd_20 = row[row["CDCG20"] < 0].head(1).index.values
    trim_idx_cd_25 = row[row["CDCG25"] < 0].head(1).index.values
    trim_idx_cd_30 = row[row["CDCG30"] < 0].head(1).index.values
    trim_idx_cd_35 = row[row["CDCG35"] < 0].head(1).index.values

    return da.concatenate([
        trim_idx_cl_20,
        trim_idx_cl_25,
        trim_idx_cl_30,
        trim_idx_cl_35,
        trim_idx_cd_20,
        trim_idx_cd_25,
        trim_idx_cd_30,
        trim_idx_cd_35,
    ])


def get_trim_index(df):
    """Fungsi ini digunakan untuk perhitungan kondisi trim untuk CL dan CD"""

    df = df.map_partitions(_find_trim_files)

    # indexes dalam bentuk array dengan jumlah sebanyak 8 kolom saat mulai trim
    # clcg20, clcg25, clcg30, clcg35, cdcg20, cdcg25, cdcg30, cdcg35
    # banyaknya baris bergantung banyaknya file windtunnel (jumlah run number sehingga menggunakan -1 untuk reshape)
    # dan memenuhi kondisi trim
    return df.compute_chunk_sizes().compute().compute().reshape(-1, 8)


def process():
    df = load_data("run*.csv")
    indexes = get_trim_index(df)

    print(indexes)
    print(indexes[:, 0])


if __name__ == "__main__":
    process()
