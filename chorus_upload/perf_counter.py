import time


class PerformanceCounter:
    def __init__(self, total_file_count: int = None):
        self.has_total = total_file_count is not None
        self.total_file_count = total_file_count
        
        self.file_count = 0
        self.file_size = 0
        self.session_file_count = 0
        self.session_file_size = 0
        self.session_start_time = time.time()
        self.start_time = time.time()
        
    def __del__(self):
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            size_throughput = self.file_size / elapsed
            file_throughput = self.file_count / elapsed
            if self.has_total:
                percent_files = (self.file_count / self.total_file_count) * 100
                remaining_time = (elapsed / self.file_count) * (self.total_file_count - self.file_count) 
                print("Total elapsed: ", elapsed, " seconds, with ",
                    self.file_count, " files (", file_throughput, " files/s) and ", 
                    self.file_size, " bytes (", size_throughput, " bytes/s). ", 
                    percent_files, "% files and ",
                    "Estimated remaining time: ", remaining_time, " seconds")
            else:
                print("Total elapsed: ", elapsed, " seconds, with ",
                    self.file_count, " files (", file_throughput, " files/s) and ", 
                    self.file_size, " bytes (", size_throughput, " bytes/s). ")
        else:
            print("Warning: total elapsed time is 0")

    def add_file(self, file_size):
        self.file_size += file_size
        self.file_count += 1
        self.session_file_count += 1
        self.session_file_size += file_size

    def report(self):
        # first compute the throughput for the session
        elapsed = time.time() - self.session_start_time
        if elapsed > 0:
            size_throughput = self.session_file_size / elapsed
            file_throughput = self.session_file_count / elapsed
            print("Session elapsed: ", elapsed, " seconds, with ", 
                  self.session_file_count, " files (", file_throughput, " files/s) and ",
                  self.session_file_size, " bytes (", size_throughput, " bytes/s)")
        else:
            print("Warning: session elapsed time is 0")

        elapsed = time.time() - self.start_time
        if elapsed > 0:
            size_throughput = self.file_size / elapsed
            file_throughput = self.file_count / elapsed
            if self.has_total:
                percent_files = (self.file_count / self.total_file_count) * 100
                remaining_time = (elapsed / self.file_count) * (self.total_file_count - self.file_count) 
                print("Cumulative elapsed: ", elapsed, " seconds, with ",
                    self.file_count, " files (", file_throughput, " files/s) and ", 
                    self.file_size, " bytes (", size_throughput, " bytes/s). ", 
                    percent_files, "% files and ",
                    "Estimated remaining time: ", remaining_time, " seconds")
            else:
                print("Cumulative elapsed: ", elapsed, " seconds, with ",
                    self.file_count, " files (", file_throughput, " files/s) and ", 
                    self.file_size, " bytes (", size_throughput, " bytes/s). ")
        else:
            print("Warning: cumulative elapsed time is 0")

            
