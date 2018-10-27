#include "Logging.hpp"

bftlogger::Logger globalLogger =
    bftlogger::Logger::getLogger(DEFAULT_LOGGER_NAME);

#ifndef USE_LOG4CPP
std::string bftlogger::SimpleLoggerImpl::LEVELS_STRINGS[6] =
    {"TRACE",
      "DEBUG",
      "INFO",
      "WARN",
      "ERROR",
      "FATAL"};

void get_time(std::stringstream &ss) {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  auto timer = system_clock::to_time_t(now);
  std::tm bt = *std::localtime(&timer);
  ss << std::put_time(&bt, "%F %T")
     << "."
     << std::setfill('0') << std::setw(3) << ms.count();
}

void bftlogger::SimpleLoggerImpl::print(
    bftlogger::LogLevel l,
    std::string text) {

  std::stringstream time;

  get_time(time);

  printf("%s %s (%s) %s\n",
         SimpleLoggerImpl::LEVELS_STRINGS[l].c_str(),
         time.str().c_str(),
         _name.c_str(),
         text.c_str());

}
#endif