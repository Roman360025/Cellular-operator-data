import csv

h_max_speed = {}
# name_of_directory = 'Assignment with a cellular operator/'
name_of_directory = 'test/'


def mapper(log_file):
    log_file = name_of_directory + log_file
    h = log_file.split('.')[1]

    global h_max_speed

    if h not in h_max_speed:
        h_max_speed[h] = 0

    with open(log_file, newline='') as File:
        day = 1
        average_in_day = []
        reader = csv.reader(File, delimiter=',')
        next(reader)  # Пропускаем первую строку
        first_row = next(reader)
        first_time, bytes = float(first_row[1][1:]), float(first_row[2][1:])  # Получаем первый отсчёт и количество байт

        if bytes > h_max_speed[h]:
            h_max_speed[h] = bytes

        for row in reader:
            if float(row[2][1:]) > h_max_speed[h]:
                h_max_speed[h] = bytes
            if int(row[0]) != day:
                day = int(row[0])
                average_in_day.append(bytes / (last_time - first_time))
                first_time, bytes = float(row[1][1:]), float(row[2][1:])
                continue
            bytes += float(row[2][1:])
            last_time = float(row[1][1:])

    average_in_day.append(bytes / (last_time - first_time))  # Учитываем последнюю строку
    return sum(average_in_day) / 7


def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


def reducer(list):
    return list.index(max(list))


def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk)

    return reducer_of_chunk(list(mapped_chunk))


def reducer_of_chunk(some_h):
    return sum(some_h) / len(some_h)
    # print(some_h)
    # sum_of_average_speed = 0
    # for i in some_h:
    #     sum_of_average_speed += i[0]
    # return sum_of_average_speed / len(some_h)

