#ifndef TIMER_H
#define TIMER_H

#include <sys/time.h>
#include <time.h>
namespace emumba {
namespace utils {
   class timer
   {
    private:
      struct timespec start_time, end_time;

    public:
      timer()
      {}
      inline void start()
      {
         clock_gettime(CLOCK_MONOTONIC, &start_time);
      }
      inline void stop()
      {
         clock_gettime(CLOCK_MONOTONIC, &end_time);
      }
      inline double get_elapsed_ms()
      {
         return get_elapsed_ns() / 1000000.0;
      }
      inline double get_elapsed_us()
      {
         return get_elapsed_ns() / 1000.0;
      }
      double get_elapsed_ns()
      {
         return ((1000000000.0 * static_cast<double>(end_time.tv_sec - start_time.tv_sec)) +
                 static_cast<double>(end_time.tv_nsec - start_time.tv_nsec));
      }
   };

}  // namespace utils
}  // namespace emumba

#endif