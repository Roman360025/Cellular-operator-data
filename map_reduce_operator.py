import csv
import pandas as pd
import matplotlib.pyplot as plt

class HOperator:

    def __init__(self):
        # self.name_of_directory = 'test/'
        self.name_of_directory = 'Assignment with a cellular operator/'
        self.new_directory = 'New log files/'
        self.result_directory = 'Result/'

    def mapper(self, log_file):
        log_file = self.new_directory + log_file
        df = pd.read_csv(log_file)

    def chunkIt(self, seq, num):
        avg = len(seq) / float(num)
        out = []
        last = 0.0

        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out

    def reducer(self, log_file):
        df = pd.read_csv(log_file)
        # df.pivot_table(values='Speed', index='Day', aggfunc='median').plot(kind='line')
        df = df.pivot_table(values='Speed', index='Day', aggfunc='median')
        return df


    def chunks_mapper(self, chunk):
        # mapped_chunk = map(self.mapper, chunk)
        name_of_h = chunk[0].split('.')[1]
        general_df = pd.read_csv(self.new_directory + chunk.pop())

        for i in chunk:
            df = pd.read_csv(self.new_directory + i)
            # general_df = pd.merge(general_df, df, on = ['Day', 'Time'], how='outer')
            general_df = pd.concat([df, general_df], axis=0)

        general_df = general_df.groupby(['Day', 'Time']).sum()
        new_name_of_file = 'Result/' + name_of_h + '.csv'
        general_df.to_csv(new_name_of_file)
        return self.reducer(new_name_of_file)

    def reducer_of_chunk(self, some_h):
        pass
        # sum_of_average_speed = 0
        # for i in some_h:
        #     sum_of_average_speed += i[0]
        # return sum_of_average_speed / len(some_h)

    def process_files(self, log_file):
        old_log_file = self.name_of_directory + log_file
        new_log_file = self.new_directory + log_file
        with open(old_log_file, newline='') as File:
            with open(new_log_file, 'w', newline='') as NewFile:
                day = 1
                reader = csv.reader(File, delimiter=',')
                writer = csv.writer(NewFile, delimiter=',')

                writer.writerow(["Day", 'Time', 'Speed'])
                next(reader)  # Пропускаем первую строку
                first_row = next(reader)
                first_minute, speed = int(float(first_row[1][1:]) / 60), float(
                    first_row[2][1:])  # Получаем первый отсчёт и количество байт

                number_speeds_in_minute = 1

                for row in reader:
                    minute = int(float(row[1][1:]) / 60)

                    if int(row[0]) != day or minute != first_minute:
                        average_in_minute = speed / number_speeds_in_minute
                        writer.writerow([day, first_minute, average_in_minute])
                        first_minute = minute
                        day = int(row[0])
                        speed = 0
                        speed += float(row[2][1:])
                        number_speeds_in_minute = 1
                    else:
                        speed += float(row[2][1:])
                        number_speeds_in_minute += 1

                average_in_minute = speed / number_speeds_in_minute
                writer.writerow([day, first_minute, average_in_minute])

    def chunk_process(self, chunk):
        list(map(self.process_files, chunk))
