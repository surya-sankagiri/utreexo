import csv
import numpy as np
import matplotlib.pyplot as plt


with open('patriciasizedatacomplete.csv', newline='') as csvfile:
    our_data = csv.DictReader(csvfile, delimiter=',', quotechar='|')

    our_data = [int(d["zlib"]) for d in our_data]


with open('utreexosizedatacomplete.csv', newline='') as csvfile:
    utreexo_data = csv.DictReader(csvfile, delimiter=',', quotechar='|')

    utreexo_data = [int(d["zlib"]) for d in utreexo_data]


min_data_len = min(len(utreexo_data), len(our_data))
# min_data_len = 500000

utreexo_data = utreexo_data[:min_data_len]
our_data = our_data[:min_data_len]

print(len(our_data), len(utreexo_data))
assert len(our_data) == len(utreexo_data)


SMOOTH = 1000

our_data_smoothed = [sum(our_data[i:i+SMOOTH])/(1000*SMOOTH) for i in range(len(our_data) - SMOOTH)]

utreexo_data_smoothed = [sum(utreexo_data[i:i+SMOOTH])/(1000*SMOOTH) for i in range(len(utreexo_data) - SMOOTH)]

print("ours", sum(our_data))
print("theirs", sum(utreexo_data))
print("ratio", sum(our_data)/sum(utreexo_data))

plt.figure(figsize=(10, 6))
plt.plot(range(len(our_data_smoothed)), our_data_smoothed, label="Ours")
plt.plot(range(len(utreexo_data_smoothed)), utreexo_data_smoothed, label="UTREEXO")
plt.legend()
plt.xlabel("Block #")
plt.ylabel("Kilobytes per block (smoothed)")
plt.tight_layout()

plt.savefig('plot.pdf')
# plt.show()
