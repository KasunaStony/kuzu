#pragma once

#include <memory>

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class PhysicalPlan {

public:
    explicit PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator, bool readOnly)
        : lastOperator{move(lastOperator)}, readOnly{readOnly} {}

    inline bool isReadOnly() { return readOnly; }

    inline bool isCopyCSV() {
        return lastOperator->getChild(0)->getOperatorType() == COPY_REL_CSV ||
               lastOperator->getChild(0)->getOperatorType() == COPY_NODE_CSV;
    }

public:
    unique_ptr<PhysicalOperator> lastOperator;
    bool readOnly;
};

} // namespace processor
} // namespace graphflow
