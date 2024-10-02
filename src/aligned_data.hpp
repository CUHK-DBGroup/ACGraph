
template <typename T, size_t N>
struct AlignedData : public T {
  static_assert(N >= sizeof(T),
                "AlignedData size must be greater than or equal to the size of the type");
  template <typename... Args>
  AlignedData(Args &&...args) : T(std::forward<Args>(args)...) {}

  char data[N - sizeof(T)];
};