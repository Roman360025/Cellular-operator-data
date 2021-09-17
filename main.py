import os
from multiprocessing import Pool
import map_reduce_operator
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
        i = i.to_dict()
        i[name_of_h[index]] = i.pop('Speed')
        result[index] = i
        index += 1


    # Первая метрика: медиана скоростей по дням
    plt.xlabel('Дни')
    plt.ylabel('Медианные скорости')
    plt.title('Скорости складывались по всем пользователям в одну и ту же минуту')
    plt.legend()
    plt.savefig("graph.png")

    plt.clf()

    #Вторая метрика: сумма максимальных скоростей по дням
    result = pool.map(operator.chunk_sum_max_speed, result)

    index = 0
    for i in result:
        week = i[name_of_h[index]]
        plt.plot([day for day in week],
                 [week[day] for day in week],
                 label=name_of_h[index])
        index += 1

    plt.xlabel('Дни')
    plt.ylabel('Сумма максимальных скоростей')
    plt.title('Максимальная скорости от станций, сумма')
    plt.legend()
    plt.savefig("max_speed_sum.png")
    plt.clf()
#     Третья метрика : наивысшие скорости по дням
    data = sorted(os.listdir(operator.result_directory))
    data_chunks = operator.chunkIt(data, number_of_chunks)

    result = pool.map(operator.chunk_max_speed, data_chunks)


    index = 0
    for i in result:
        plt.plot([day for day in i.to_dict()],
                 [i.to_dict()[day] for day in i.to_dict()],
                 label=name_of_h[index])
        index += 1

    plt.xlabel('Дни')
    plt.ylabel('Максимальные скорости')
    plt.title('Максимальная скорость по дням суммарно от всех пользователей')
    plt.legend()
    plt.savefig("max_speed.png")



