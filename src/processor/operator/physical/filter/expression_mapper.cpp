#include "src/processor/include/operator/physical/filter/expression_mapper.h"

#include "src/common/include/expression_type.h"
#include "src/expression/include/logical/logical_expression.h"
#include "src/expression/include/physical/physical_binary_expression.h"
#include "src/expression/include/physical/physical_expression.h"
#include "src/expression/include/physical/physical_unary_expression.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysicalOne(
    unique_ptr<LogicalExpression> expression);

static unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysicalOne(
    unique_ptr<LogicalExpression> expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    DataChunks& dataChunks);

unique_ptr<PhysicalExpression> ExpressionMapper::mapLogicalToPhysical(
    unique_ptr<LogicalExpression> expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    DataChunks& dataChunks) {
    auto expressionType = expression->getExpressionType();
    if (isExpressionLeafLiteral(expressionType)) {
        return mapLogicalLiteralExpressionToPhysicalOne(move(expression));
    } else if (isExpressionLeafVariable(expressionType)) {
        return mapLogicalPropertyExpressionToPhysicalOne(
            move(expression), physicalOperatorInfo, dataChunks);
    } else if (isExpressionUnary(expressionType)) {
        auto child =
            mapLogicalToPhysical(expression->getChildExpr(0), physicalOperatorInfo, dataChunks);
        return make_unique<PhysicalUnaryExpression>(move(child), expression->getExpressionType());
    } else if (isExpressionBinary(expressionType)) {
        auto leftExpr =
            mapLogicalToPhysical(expression->getChildExpr(0), physicalOperatorInfo, dataChunks);
        auto rightExpr =
            mapLogicalToPhysical(expression->getChildExpr(1), physicalOperatorInfo, dataChunks);
        return make_unique<PhysicalBinaryExpression>(
            move(leftExpr), move(rightExpr), expression->getExpressionType());
    }
    // should never happen.
    throw std::invalid_argument("Unsupported expression type.");
}

unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysicalOne(
    unique_ptr<LogicalExpression> expression) {
    auto dataType = expression->getDataType();
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto dataChunkForLiteral = make_shared<DataChunk>();
    dataChunkForLiteral->size = 1;
    dataChunkForLiteral->currPos = 0;
    auto valueVector = make_shared<ValueVector>(dataType, 1 /* capacity */);
    valueVector->setDataChunkOwner(dataChunkForLiteral);
    switch (dataType) {
    case INT: {
        valueVector->setValue(0, expression->getLiteralValue().primitive.integer);
    } break;
    case DOUBLE: {
        valueVector->setValue(0, expression->getLiteralValue().primitive.double_);
    } break;
    case BOOL:
        valueVector->setValue(0, expression->getLiteralValue().primitive.boolean);
        break;
    default:
        throw std::invalid_argument("Unsupported data type for literal expressions.");
    }
    return make_unique<PhysicalExpression>(valueVector);
}

unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysicalOne(
    unique_ptr<LogicalExpression> expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    DataChunks& dataChunks) {
    auto variableName = expression->getVariableName();
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(variableName);
    auto dataChunk = dataChunks.getDataChunk(dataChunkPos);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(variableName);
    auto valueVector = dataChunk->getValueVector(valueVectorPos);
    return make_unique<PhysicalExpression>(valueVector);
}

} // namespace processor
} // namespace graphflow