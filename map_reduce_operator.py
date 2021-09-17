import csv
import pandas as pd


class HOperator:

    def __init__(self):
        # self.name_of_directory = 'test/'
        self.name_of_directory = 'Assignment with a cellular operator/'
        self.new_directory = 'New log files/'
        self.result_directory = 'Result/'

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
        df = df.pivot_table(values='Speed', index='Day', aggfunc='median')
        return df

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

    def chunks_mapper(self, chunk):
        name_of_h = chunk[0].split('.')[1]
        general_df = pd.read_csv(self.new_directory + chunk.pop())

        for i in chunk:
            df = pd.read_csv(self.new_directory + i)
            general_df = pd.concat([df, general_df], axis=0)

        general_df = general_df.groupby(['Day', 'Time']).sum()
        new_name_of_file = 'Result/' + name_of_h + '.csv'
        general_df.to_csv(new_name_of_file)
        return self.reducer(new_name_of_file)

    def chunk_max_speed(self, chunk):
        df = pd.read_csv(self.result_directory + chunk[0])
        df = df.groupby(["Day"])["Speed"].max()
        return df
