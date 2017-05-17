#ifndef RAII_H
#define RAII_H
template <typename T, typename D = int (*)(T)> class raii
{
  private:
    T ptr;
    D dest;

  public:
    raii() = delete;
    raii(const raii &) = delete;
    raii &operator=(const raii &) = delete;
    raii(D x) : ptr{nullptr}, dest{x} {}
    ~raii() { dest(ptr); }
    inline operator T() { return ptr; }
    inline T *operator&() { return &ptr; }
};
#endif
