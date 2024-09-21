import time


class PerformanceCounter:
    def __print_total(self):
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            size = self.file_size / (1024 * 1024)
            size_throughput = size / elapsed
            file_throughput = self.file_count / elapsed
            if self.has_total:
                percent_files = (self.file_count / self.total_file_count) * 100
                remaining_time = (elapsed / self.file_count) * (self.total_file_count - self.file_count) 
                print("Total elapsed: {:.2f} seconds, with {} files ({:.2f} files/s) and ".format(
                    elapsed, self.file_count, file_throughput) + \
                    " {:.2f} MB ({:.2f} MB/s). {:.2f}% files and Estimated remaining time: {:.2f} seconds".format(
                    size, size_throughput, percent_files, remaining_time))
            else:
                print("Total elapsed: {:.2f} seconds, with {} files ({:.2f} files/s) and {:.2f} MB ({:.2f} MB/s).".format(
                    elapsed, self.file_count, file_throughput, size, size_throughput))
        else:
            print("Warning: total elapsed time is 0")
            
    def __print_session(self):
        elapsed = time.time() - self.session_start_time
        if elapsed > 0:
            size = float(self.session_file_size) / (1024 * 1024)
            size_throughput = size / elapsed
            file_throughput = float(self.session_file_count) / elapsed
            print("Session elapsed: {:.2f} seconds, with {} files ({:.2f} files/s) and {:.2f} MB ({:.2f} MB/s)".format(
                elapsed, self.session_file_count, file_throughput, size, size_throughput))
        else:
            print("Warning: session elapsed time is 0")        

    
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
        self.__print_total()
        
    def add_file(self, file_size):
        self.file_size += file_size
        self.file_count += 1
        self.session_file_count += 1
        self.session_file_size += file_size

    def report(self):
        # first compute the throughput for the session
        self.__print_session()
        self.session_file_size = 0
        self.session_file_count = 0
        self.session_start_time = time.time()

        self.__print_total()
            
