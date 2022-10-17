#include "src/processor/include/physical_plan/operator/unwind.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Unwind::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    expressionEvaluator->init(*resultSet, context->memoryManager);
    outValueVector = make_shared<ValueVector>(*outDataType, context->memoryManager);
    resultSet->dataChunks[outDataPos.dataChunkPos]->insert(
        outDataPos.valueVectorPos, outValueVector);
    return resultSet;
}

bool Unwind::hasMoreToRead() const {
    return inputList.size > startIndex;
}

void Unwind::copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const {
    auto numOfBytes = Types::getDataTypeSize(outDataType->typeID);
    for (auto pos = startPos; pos < endPos; pos++) {
        ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(*outValueVector, pos - startPos,
            reinterpret_cast<uint8_t*>(inputList.overflowPtr) + pos * numOfBytes);
    }
}

bool Unwind::getNextTuples() {
    metrics->executionTime.start();
    if (hasMoreToRead()) {
        auto totalElementsCopy = min(DEFAULT_VECTOR_CAPACITY, inputList.size - startIndex);
        copyTuplesToOutVector(startIndex, (totalElementsCopy + startIndex));
        startIndex += totalElementsCopy;
        outValueVector->state->initOriginalAndSelectedSize(totalElementsCopy);
        metrics->executionTime.stop();
        return true;
    }
    do {
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        expressionEvaluator->evaluate();
        auto currIndex = expressionEvaluator->resultVector->state->currIdx;
        if (expressionEvaluator->resultVector->isNull(currIndex)) {
            outValueVector->state->selVector->selectedSize = 0;
            continue;
        }
        inputList = ((gf_list_t*)(expressionEvaluator->resultVector->values))[currIndex];
        startIndex = 0;
        auto totalElementsCopy = min(DEFAULT_VECTOR_CAPACITY, inputList.size);
        copyTuplesToOutVector(0, totalElementsCopy);
        startIndex += totalElementsCopy;
        outValueVector->state->initOriginalAndSelectedSize(startIndex);
    } while (outValueVector->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
