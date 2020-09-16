from dask import (dataframe as dd, array as da)


def search_multiple_index(df):
    """Fungsi ini digunakan sebagai fungsi bantuan dalam pencarian index saat terjadi trim untuk masing-masing data
    windtunnel (jumlah run number)"""

    # temukan urutan (0 based) dari index saat trim untuk masing-masing file windtunnel (index < 0)
    trim_idx = da.diff(df).argmin() + 1  # karena 0 based jadi perlu ditambahkan 1

    # ambil nilai sebenarnya berdasarkan index yang terpilih
    trim_idx = da.take(df, da.concatenate([[0], [trim_idx]]))

    return trim_idx


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


def get_trim_index(df):
    """
    Fungsi untuk mencari index saat terjadi kondisi trim. Fungsi ini juga bersifat lazy sehingga untuk mendapatkan hasil
    perhitungan harus memanggil perintah compute()
    """

    # Index dari dataframe df (CL trim) dimana CM bernilai negatif disimpan kedalam Array
    clcg20 = df[df["CLCG20"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)
    clcg25 = df[df["CLCG25"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)
    clcg30 = df[df["CLCG30"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)
    clcg35 = df[df["CLCG35"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)

    # Index dari dataframe df (CD trim) dimana CM bernilai negatif disimpan kedalam Array
    cdcg20 = df[df["CDCG20"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)
    cdcg25 = df[df["CDCG25"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)
    cdcg30 = df[df["CDCG30"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)
    cdcg35 = df[df["CDCG35"] < 0].iloc[:, 0].index.to_dask_array(lengths=True)

    # indexes dalam bentuk array dengan jumlah sebanyak 8 kolom saat mulai trim
    # clcg20, clcg25, clcg30, clcg35, cdcg20, cdcg25, cdcg30, cdcg35
    # banyaknya baris bergantung banyaknya file windtunnel (jumlah run number) dan memenuhi kondisi trim
    trim_idx_cl_20 = search_multiple_index(clcg20).reshape(2, 1)
    trim_idx_cl_25 = search_multiple_index(clcg25).reshape(2, 1)
    trim_idx_cl_30 = search_multiple_index(clcg30).reshape(2, 1)
    trim_idx_cl_35 = search_multiple_index(clcg35).reshape(2, 1)
    trim_idx_cd_20 = search_multiple_index(cdcg20).reshape(2, 1)
    trim_idx_cd_25 = search_multiple_index(cdcg25).reshape(2, 1)
    trim_idx_cd_30 = search_multiple_index(cdcg30).reshape(2, 1)
    trim_idx_cd_35 = search_multiple_index(cdcg35).reshape(2, 1)

    # Kombinasi keseluruhan index trim
    comb_trim = da.concatenate([
        trim_idx_cl_20,
        trim_idx_cl_25,
        trim_idx_cl_30,
        trim_idx_cl_35,
        trim_idx_cd_20,
        trim_idx_cd_25,
        trim_idx_cd_30,
        trim_idx_cd_35,
    ], axis=1)

    return comb_trim


def process():
    df = load_data("run*.csv")
    indexes = get_trim_index(df)

    print(indexes.compute())


if __name__ == "__main__":
    process()
