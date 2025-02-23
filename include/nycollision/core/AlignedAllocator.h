#pragma once

#include <cstdlib>    // for std::aligned_alloc, std::free
#include <new>        // for std::bad_alloc
#include <limits>     // for std::numeric_limits


template <class T, std::size_t Alignment>
class AlignedAllocator {
public:
    using value_type      = T;
    using pointer         = T*;
    using const_pointer   = const T*;
    using reference       = T&;
    using const_reference = const T&;
    using size_type       = std::size_t;
    using difference_type = std::ptrdiff_t;

    // Required for older C++ implementations: rebind
    template <class U>
    struct rebind {
        using other = AlignedAllocator<U, Alignment>;
    };

    AlignedAllocator() noexcept = default;
    template <class U>
    AlignedAllocator(const AlignedAllocator<U, Alignment>&) noexcept {}

    pointer allocate(size_type n) {
       //Alignment must be a multiple of sizeof(void*)"

        std::size_t bytes = n * sizeof(T);
     
        if (bytes % Alignment != 0) {
            bytes += (Alignment - (bytes % Alignment));
        }

        void* p = std::aligned_alloc(Alignment, bytes);
        if (!p) throw std::bad_alloc();
        return static_cast<pointer>(p);
    }

    void deallocate(pointer p, size_type) noexcept {
        std::free(p);
    }

    // Comparison operators
    bool operator==(const AlignedAllocator&) const noexcept { return true;  }
    bool operator!=(const AlignedAllocator&) const noexcept { return false; }

   
    size_type max_size() const noexcept {
        return std::numeric_limits<size_type>::max() / sizeof(T);
    }
};
