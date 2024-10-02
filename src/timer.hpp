#pragma once

#include <array>
#include <chrono>
#include <atomic>

enum struct TIMER : size_t {
  ALL, GRAPH_INIT, CP_INIT, SC_INIT, CALC, LOAD, OUTPUT, INIT, REST, DECONSTRUCT, _
};

// const char *timer_name[] = {
//   "ALL", "GRAPH_INIT", "CP_INIT", "SC_INIT", "CALC", "LOAD", "OUTPUT", "INIT", "REST", "DECONSTRUCT"
// };

template<typename Tenum = TIMER>
class TimerTemp {
private:
  static std::array<std::atomic_uint64_t , (size_t)Tenum::_> timers;

public:
  static size_t used(Tenum timer) {
    return timers[(size_t)timer].load(std::memory_order_acquire);
  }

  static void reset(Tenum timer) {
    timers[(size_t)timer].store(0, std::memory_order_release);
  }

  static void reset_all() {
    for (size_t id = 0; id < (size_t)Tenum::_; ++id) reset((Tenum)id);
  }

private:
  using clock = std::chrono::steady_clock;
  using duration = std::chrono::microseconds;

  const size_t _timer_id;
  const clock::time_point _setup_time;

public:
  TimerTemp(Tenum timer) :
    _timer_id((size_t)timer),
    _setup_time(clock::steady_clock::now()) { }

  ~TimerTemp() {
    auto duration_time = std::chrono::duration_cast<duration>(clock::now() - _setup_time);
    timers[_timer_id].fetch_add(duration_time.count(), std::memory_order_acq_rel);
    timers[(size_t)TIMER::ALL].fetch_add(duration_time.count(), std::memory_order_acq_rel);
    // log_info("%s: %.3lfms", timer_name[_timer_id], duration_time.count()/1e3);
  }
};

template<typename Tenum>
std::array<std::atomic_uint64_t, (size_t)Tenum::_> TimerTemp<Tenum>::timers { };

using Timer = TimerTemp<TIMER>;