#include "include/function_evaluator.h"

#include "src/binder/expression/include/function_expression.h"

namespace graphflow {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    execFunc = ((ScalarFunctionExpression&)*expression).execFunc;
    if (expression->dataType == BOOL) {
        selectFunc = ((ScalarFunctionExpression&)*expression).selectFunc;
    }
    resultVector = make_shared<ValueVector>(memoryManager, expression->dataType);
    // set resultVector state to the state of its unFlat child if there is any
    assert(!children.empty());
    resultVector->state = children[0]->resultVector->state;
    for (auto& child : children) {
        if (!child->isResultVectorFlat()) {
            resultVector->state = child->resultVector->state;
            break;
        }
    }
    for (auto& child : children) {
        parameters.push_back(child->resultVector);
    }
}

void FunctionExpressionEvaluator::evaluate() {
    for (auto& child : children) {
        child->evaluate();
    }
    execFunc(parameters, *resultVector);
}

uint64_t FunctionExpressionEvaluator::select(sel_t* selectedPos) {
    for (auto& child : children) {
        child->evaluate();
    }
    return selectFunc(parameters, selectedPos);
}

unique_ptr<BaseExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    vector<unique_ptr<BaseExpressionEvaluator>> clonedChildren;
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, move(clonedChildren));
}

} // namespace evaluator
} // namespace graphflow