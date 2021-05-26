#include "src/processor/include/physical_plan/expression_mapper.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/common/include/expression_type.h"
#include "src/expression_evaluator/include/binary_expression_evaluator.h"
#include "src/expression_evaluator/include/unary_expression_evaluator.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

static unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToPhysical(
    const Expression& expression);

static unique_ptr<ExpressionEvaluator> mapLogicalPropertyOrCSVLineExtractExpressionToPhysical(
    const Expression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    ResultSet& resultSet);

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapToPhysical(const Expression& expression,
    PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet) {
    auto expressionType = expression.expressionType;
    if (isExpressionLeafLiteral(expressionType)) {
        return mapLogicalLiteralExpressionToPhysical(expression);
    } else if (isExpressionLeafVariable(expressionType)) {
        /**
         *Both CSV_LINE_EXTRACT and PropertyExpression are mapped to the same physical expression
         *evaluator, because both of them only grab data from a value vector.
         */
        return mapLogicalPropertyOrCSVLineExtractExpressionToPhysical(
            expression, physicalOperatorInfo, resultSet);
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo, resultSet);
        return make_unique<UnaryExpressionEvaluator>(
            move(child), expressionType, expression.dataType);
    } else {
        assert(isExpressionBinary(expressionType));
        auto lExpr = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo, resultSet);
        auto rExpr = mapToPhysical(expression.getChildExpr(1), physicalOperatorInfo, resultSet);
        return make_unique<BinaryExpressionEvaluator>(
            move(lExpr), move(rExpr), expressionType, expression.dataType);
    }
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::clone(
    const ExpressionEvaluator& expression, ResultSet& resultSet) {
    if (isExpressionLeafLiteral(expression.expressionType)) {
        return make_unique<ExpressionEvaluator>(expression.result, expression.expressionType);
    } else if (isExpressionLeafVariable(expression.expressionType)) {
        auto dataChunk = resultSet.dataChunks[expression.dataChunkPos];
        auto valueVector = dataChunk->getValueVector(expression.valueVectorPos);
        return make_unique<ExpressionEvaluator>(
            valueVector, expression.dataChunkPos, expression.valueVectorPos);
    } else if (expression.getNumChildrenExpr() == 1) { // unary expression.
        return make_unique<UnaryExpressionEvaluator>(clone(expression.getChildExpr(0), resultSet),
            expression.expressionType, expression.dataType);
    } else { // binary expression.
        return make_unique<BinaryExpressionEvaluator>(clone(expression.getChildExpr(0), resultSet),
            clone(expression.getChildExpr(1), resultSet), expression.expressionType,
            expression.dataType);
    }
}

unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToPhysical(
    const Expression& expression) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(
        literalExpression.storeAsPrimitiveVector ? literalExpression.dataType : UNSTRUCTURED,
        1 /* capacity */);
    vector->state = VectorState::getSingleValueDataChunkState();
    if (!literalExpression.storeAsPrimitiveVector) {
        ((Value*)vector->values)[0] = literalExpression.literal;
    } else {
        switch (expression.dataType) {
        case INT32: {
            ((int32_t*)vector->values)[0] = literalExpression.literal.primitive.int32Val;
        } break;
        case DOUBLE: {
            ((double_t*)vector->values)[0] = literalExpression.literal.primitive.doubleVal;
        } break;
        case BOOL: {
            auto val = literalExpression.literal.primitive.booleanVal;
            vector->nullMask[0] = val == NULL_BOOL;
            vector->values[0] = val;
        } break;
        case STRING: {
            ((gf_string_t*)vector->values)[0] = literalExpression.literal.strVal;
        } break;
        default:
            assert(false);
        }
    }
    return make_unique<ExpressionEvaluator>(vector, expression.expressionType);
}

unique_ptr<ExpressionEvaluator> mapLogicalPropertyOrCSVLineExtractExpressionToPhysical(
    const Expression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    ResultSet& resultSet) {
    const auto& variableName = expression.variableName;
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(variableName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(variableName);
    auto dataChunk = resultSet.dataChunks[dataChunkPos];
    auto valueVector = dataChunk->getValueVector(valueVectorPos);
    return make_unique<ExpressionEvaluator>(valueVector, dataChunkPos, valueVectorPos);
}

} // namespace processor
} // namespace graphflow
