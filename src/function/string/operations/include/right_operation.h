#pragma once

#include <cassert>
#include <cstring>

#include "substr_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Right {
public:
    static inline void operation(gf_string_t& left, int64_t& right, gf_string_t& result,
        bool isLeftNull, bool isRightNull, ValueVector& resultValueVector) {
        assert(!isLeftNull && !isRightNull);
        auto len = right > 0 ? min(left.len, (uint32_t)right) : max(0l, left.len + right);
        SubStr::operation(left, left.len - len + 1, len, result, false /* isLeftNull*/,
            false /* isStartNull*/, false /*isLenNull */, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
