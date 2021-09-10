import os
from multiprocessing import Pool
from map_reduce_operator import chunkIt, reducer, chunks_mapper, name_of_directory
import map_reduce_operator

if __name__ == "__main__":
    number_of_chunks = 2

    pool = Pool(number_of_chunks)
    data = sorted(os.listdir(name_of_directory))
    data_chunks = chunkIt(data, number_of_chunks)

    name_of_h = {}

    for i in data_chunks:
        name_of_h[data_chunks.index(i)] = i[0].split('.')[1]

    mapped = pool.map(chunks_mapper, data_chunks)
    print(list(mapped))
    print('Список средних скоростей')

    for i in list(mapped):
        print(name_of_h[list(mapped).index(i)],  ":", i)

    print()
    reduce = reducer(list(mapped))
    print('Наивысшая средняя скорость:', name_of_h[reduce])
