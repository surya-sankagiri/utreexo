import csv
import numpy as np
import matplotlib.pyplot as plt


with open('patriciasizedata.csv', newline='') as csvfile:
    our_data = csv.reader(csvfile, delimiter=',', quotechar='|')

    our_data = [int(d[1]) for d in our_data]


with open('utreexosizedata.csv', newline='') as csvfile:
    utreexo_data = csv.reader(csvfile, delimiter=',', quotechar='|')

    utreexo_data = [int(d[1]) for d in utreexo_data]


min_data_len = min(len(utreexo_data), len(our_data))

utreexo_data = utreexo_data[:min_data_len]
our_data = our_data[:min_data_len]

print(len(our_data), len(utreexo_data))
assert len(our_data) == len(utreexo_data)

SMOOTH = 1000

our_data_smoothed = [sum(our_data[i:i+SMOOTH])/SMOOTH for i in range(len(our_data) - SMOOTH)]

utreexo_data_smoothed = [sum(utreexo_data[i:i+SMOOTH])/SMOOTH for i in range(len(utreexo_data) - SMOOTH)]

print(sum(our_data)/sum(utreexo_data))


plt.plot(range(len(our_data_smoothed)), our_data_smoothed, label="Ours")
plt.plot(range(len(utreexo_data_smoothed)), utreexo_data_smoothed, label="Utreexo")
plt.legend()
plt.xlabel("Block #")
plt.ylabel("Bytes per block (smoothed)")

plt.show()
