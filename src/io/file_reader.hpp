class FileReader {
public:
 static void read(int fd, char *buf, size_t nbyte, size_t offset) {
   total_read_size.fetch_add(nbyte);
   if (nbyte == 4096) {
     num_read_blocks.fetch_add(1);
   }
   assert(
     static_cast<size_t>(pread(fd, buf, nbyte, offset)) == nbyte
   );
 }

  static void init(const std::string &filepath) {
    return;
    Timer *timer = new Timer(TIMER::LOAD);
    int fd = open(filepath.c_str(), O_RDONLY);
    assert(fd > 0);
    size_t filesize = std::filesystem::file_size(filepath);
    data = (char *) std::aligned_alloc(4096, filesize);
    size_t rd_size = 0;
    while (rd_size < filesize) {
      auto t = pread(fd, data + rd_size, filesize - rd_size, rd_size);
      assert(t > 0);
      rd_size += t;
    }
    delete timer;
    log_info("loading use %lfms", Timer::used(TIMER::LOAD)/1e3);
  }

  // static void read(int fd, char *buf, size_t nbyte, size_t offset) {
  //   memcpy(buf, data+offset, nbyte);
  // }

  template<class F>
  static void read_aio(int fd, char *buf, size_t nbyte, size_t offset, const F &callback) {
    callback();
  }

  struct IOStat {
    size_t total_read_size, num_read_blocks;
  };
  static IOStat statistic() {
    return {total_read_size.load(), num_read_blocks.load()};
  }

private:
  static char *data;
  static std::atomic_uint64_t read_num;
  static std::atomic_uint64_t total_read_size;
  static std::atomic_uint64_t num_read_blocks;
};

char *FileReader::data = nullptr;
std::atomic_uint64_t FileReader::read_num = 0;
std::atomic_uint64_t FileReader::total_read_size = 0;
std::atomic_uint64_t FileReader::num_read_blocks = 0;
