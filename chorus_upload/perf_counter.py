import time

import logging
log = logging.getLogger(__name__)


class PerformanceCounter:
    def __print_total(self):
        cur_t = time.time()
        elapsed = cur_t - self.start_time
        if (elapsed > 0):
            size = self.file_size / (1024 * 1024)
            size_throughput = size / elapsed
            file_throughput = self.file_count / elapsed
            if (self.has_total) and (self.total_file_count > 0) and (self.file_count > 0):
                percent_files = (self.file_count / self.total_file_count) * 100
                remaining_time = (elapsed / self.file_count) * (self.total_file_count - self.file_count) 
                log.info(f"TIMING: {cur_t:.2f} Total elapsed: {elapsed:.2f} seconds, with {self.file_count} files ({file_throughput:.2f} files/s) and " +
                    f" {size:.2f} MB ({size_throughput:.2f} MB/s). {percent_files:.2f}% files and Estimated remaining time: {remaining_time:.2f} seconds")
                
            else:
                log.info(f"TIMING: {cur_t:.2f} Total elapsed: {elapsed:.2f} seconds, with {self.file_count} files ({file_throughput:.2f} files/s) and {size:.2f} MB ({size_throughput:.2f} MB/s).")
        else:
            log.warning(f"TIMING: {cur_t:.2f} Warning: total elapsed time is 0")
            
    def __print_session(self):
        cur_t = time.time()
        elapsed = cur_t - self.session_start_time
        if elapsed > 0:
            size = float(self.session_file_size) / (1024 * 1024)
            size_throughput = size / elapsed
            file_throughput = float(self.session_file_count) / elapsed
            log.info(f"TIMING: {cur_t:.2f} Session elapsed: {elapsed:.2f} seconds, with {self.session_file_count} files ({file_throughput:.2f} files/s) and {size:.2f} MB ({size_throughput:.2f} MB/s)")
        else:
            log.warning(f"TIMING: {cur_t:.2f} Warning: session elapsed time is 0")        

    
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
            
