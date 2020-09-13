import os
import copy
import numpy
import openpyxl
from openpyxl.utils import range_boundaries

wt_ori = {
    "alpha": [],
    "beta": [],
    "cl": [],
    "cd": [],
    "cm25": [],
    "cy": [],
    "cyaw": [],
    "croll": []
}

trim = copy.deepcopy(wt_ori)

vtp = {
    "v": [],
    "power": [],
    "thrust": [],
    "CLperCD": [],
}

mass = 1300
rho = 1.225
A = 12.8

cg_default = 25

n = 0

y = 0

inc = 2.5

file_wt = input("Masukkan Nama File:")

min_col, min_row, max_col, max_row = range_boundaries("A:H")
wb = openpyxl.load_workbook(str(file_wt) + '.xlsx', read_only=True)
wb.active = 0
wt_ws = wb.active
x = 0

for row in wt_ws.iter_rows():
    x += 1
    if x == 1:
        continue
    wt_ori["alpha"].append(float(row[0].value))
    wt_ori["beta"].append(float(row[1].value))
    wt_ori["cl"].append(float(row[2].value))
    wt_ori["cd"].append(float(row[3].value))
    wt_ori["cm25"].append(float(row[4].value))
    wt_ori["cy"].append(float(row[5].value))
    wt_ori["cyaw"].append(float(row[6].value))
    wt_ori["croll"].append(float(row[7].value))

wt_mod = copy.deepcopy(wt_ori)

option = input("Apakah ingin memasukkan variasi CG? (Y/N)")
if option == "Y" or option == "y":
    cg_min = int(input("Masukan nilai CG min (%)"))
    cg_max = int(input("Masukan nilai CG max (%)"))


else:
    cg_min = cg_default
    cg_max = cg_default

for increment in numpy.arange(cg_min, (cg_max + inc), inc):
    for x in range(0, len(wt_mod["cm25"])):
        wt_mod["cm25"][x] = wt_ori["cm25"][x] + (increment - cg_default) / 100 * wt_ori["cl"][x]

    for x in range(0, len(wt_mod["cm25"]) - 1):
        n = x
        if (wt_mod["cm25"][x] * wt_mod["cm25"][x + 1]) < 0:
            break

    # Mendeteksi dua nilai terakhir pasangan posttif negatif atau tidak
    if (wt_mod["cm25"][x] * wt_mod["cm25"][x + 1]) > 0:
        print("\nCG " + str(increment) + "%: Belum mencapai Cm trim\n")
        continue

    for name in wt_mod.keys():
        z = ((wt_mod[name][n + 1] - wt_mod[name][n]) / (wt_mod["cm25"][n + 1] - wt_mod["cm25"][n]) * (
            -wt_mod["cm25"][n])) + wt_mod[name][n]
        trim[name].append(z)

    vtp["v"].append((2 * mass * 10 / (rho * A * trim["cl"][y])) ** (1 / 2))
    Vtrim = vtp["v"][y]

    vtp["thrust"].append(0.5 * rho * (Vtrim * Vtrim) * A * trim["cd"][y])
    Ttrim = vtp["thrust"][y]

    vtp["power"].append(Vtrim * Ttrim)
    Ptrim = vtp["power"][y]

    vtp["CLperCD"].append(trim["cl"][y] / trim["cd"][y])
    CLperCD_trim = vtp["CLperCD"][y]

    print("\nCG " + str(increment) + "%:\n")

    for name, data in trim.items():
        print(name.title() + " trim: " + str(trim[name][y]))

    for name, data in vtp.items():
        print(name.title() + " trim: " + str(vtp[name][y]))

    y += 1

# Looping sampai mendapatkan Thrust Propeller > Thrust
rpm_awal = 100
# for sampai 1000 kalau Thrust Propeller > Thrust ---> Break, nilai ini yang diambil

Diameter = 2
rpm = rpm_awal + 100  # penambahan rpm per looping sebesar 100
airspeed = vtp["v"][0]
print("airspeed ", airspeed)

JJ = airspeed / (Diameter * rpm / 60)
print("Advance Ratio", JJ)

AdvanceRatio = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
Coef_Thrust = [0, 0.3, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.93]
Coef_Thrust_result = numpy.interp(JJ, AdvanceRatio, Coef_Thrust)

print("Koefisien Thrust :", Coef_Thrust_result)

Thrust_propeller = rho * Coef_Thrust_result * (rpm / 60) ** 2 * Diameter ** 4

print("Thrust propeller :", Thrust_propeller)

rpm_awal = rpm  # memasukkan nilai rpm_awal baru

# Batas Loop

os.system("pause")
