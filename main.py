import os
from multiprocessing import Pool
import map_reduce_operator
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

if __name__ == "__main__":
    number_of_chunks = 4

    pool = Pool(number_of_chunks)
    operator = map_reduce_operator.HOperator()
    data = sorted(os.listdir(operator.name_of_directory))

    data_chunks = operator.chunkIt(data, number_of_chunks)

    name_of_h = {}

    for i in data_chunks:
        name_of_h[data_chunks.index(i)] = i[0].split('.')[1]

    pool.map(operator.chunk_process, data_chunks)

    data = sorted(os.listdir(operator.new_directory))
    data_chunks = operator.chunkIt(data, number_of_chunks)

    result = pool.map(operator.chunks_mapper, data_chunks)

    index = 0
    for i in result:
        plt.plot([day for day in i.to_dict()['Speed']],
                 [i.to_dict()['Speed'][day] for day in i.to_dict()['Speed']],
                 label=name_of_h[index])
        index += 1

    # Первая метрика: медиана скоростей по дням
    plt.legend()
    plt.savefig("graph.png")

    plt.clf()

#     Вторая метрика : наивысшие скорости по дням
    data = sorted(os.listdir(operator.result_directory))
    data_chunks = operator.chunkIt(data, number_of_chunks)

    result = pool.map(operator.chunk_max_speed, data_chunks)

    print(result)

    index = 0
    for i in result:
        plt.plot([day for day in i.to_dict()],
                 [i.to_dict()[day] for day in i.to_dict()],
                 label=name_of_h[index])
        index += 1

    plt.legend()
    plt.savefig("max_speed.png")

