import csv


class H_operator:

    def __init__(self):
        self.h_max_speed = {}
        # self.name_of_directory = 'test/'
        self.name_of_directory = 'test/'
        self.name_of_directory = 'Assignment with a cellular operator/'

    def mapper(self, log_file):
        log_file = self.name_of_directory + log_file
        h = log_file.split('.')[1]

        if h not in self.h_max_speed:
            self.h_max_speed[h] = 0

        with open(log_file, newline='') as File:
            day = 1
            average_in_day = []
            reader = csv.reader(File, delimiter=',')
            next(reader)  # Пропускаем первую строку
            first_row = next(reader)
            first_time, bytes = float(first_row[1][1:]), float(
                first_row[2][1:])  # Получаем первый отсчёт и количество байт

            if bytes > self.h_max_speed[h]:
                self.h_max_speed[h] = bytes

            for row in reader:
                if float(row[2][1:]) > self.h_max_speed[h]:
                    self.h_max_speed[h] = bytes
                if int(row[0]) != day:
                    day = int(row[0])
                    average_in_day.append(bytes / (last_time - first_time))
                    first_time, bytes = float(row[1][1:]), float(row[2][1:])
                    continue
                bytes += float(row[2][1:])
                last_time = float(row[1][1:])

        average_in_day.append(bytes / (last_time - first_time))  # Учитываем последнюю строку

        self.init_speed(self.h_max_speed)
        return sum(average_in_day) / 7

    def chunkIt(self, seq, num):
        avg = len(seq) / float(num)
        out = []
        last = 0.0

        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out

    def reducer(self, list):
        return list.index(max(list))

    def chunks_mapper(self, chunk):
        mapped_chunk = map(self.mapper, chunk)
        return self.reducer_of_chunk(list(mapped_chunk))

    def reducer_of_chunk(self, some_h):
        return sum(some_h) / len(some_h)

    def init_speed(self, speed):
        self.h_max_speed = speed
