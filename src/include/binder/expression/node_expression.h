#pragma once

#include "property_expression.h"

namespace kuzu {
namespace binder {

class NodeExpression : public Expression {
public:
    NodeExpression(const string& uniqueName, unordered_set<table_id_t> tableIDs)
        : Expression{VARIABLE, NODE, uniqueName}, tableIDs{std::move(tableIDs)} {}

    inline void addTableIDs(const unordered_set<table_id_t>& tableIDsToAdd) {
        tableIDs.insert(tableIDsToAdd.begin(), tableIDsToAdd.end());
    }
    inline uint32_t getNumTableIDs() const { return tableIDs.size(); }
    inline unordered_set<table_id_t> getTableIDs() const { return tableIDs; }
    inline table_id_t getTableID() const {
        assert(tableIDs.size() == 1);
        return *tableIDs.begin();
    }

    inline string getIDProperty() const { return uniqueName + "." + INTERNAL_ID_SUFFIX; }

    inline shared_ptr<Expression> getNodeIDPropertyExpression() {
        return make_unique<PropertyExpression>(DataType(NODE_ID), INTERNAL_ID_SUFFIX,
            UINT32_MAX /* property key for internal id*/, shared_from_this());
    }

private:
    unordered_set<table_id_t> tableIDs;
};

} // namespace binder
} // namespace kuzu