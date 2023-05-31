package tools.java_api;

import java.util.Map;
public class KuzuNative {
    static {
        System.loadLibrary("kuzu_java_native");
    }

    // Database
    protected static native long kuzu_database_init(String database_path, long buffer_pool_size);
    protected static native void kuzu_database_destroy(KuzuDatabase db);
    protected static native void kuzu_database_set_logging_level(String logging_level, KuzuDatabase db);

    // Connection
    protected static native long kuzu_connection_init(KuzuDatabase database);
    protected static native void kuzu_connection_destroy(KuzuConnection connection);
    protected static native void kuzu_connection_begin_read_only_transaction(KuzuConnection connection);
    protected static native void kuzu_connection_begin_write_transaction(KuzuConnection connection);
    protected static native void kuzu_connection_commit(KuzuConnection connection);
    protected static native void kuzu_connection_rollback(KuzuConnection connection);
    protected static native void kuzu_connection_set_max_num_thread_for_exec(
        KuzuConnection connection, long num_threads);
    protected static native long kuzu_connection_get_max_num_thread_for_exec(KuzuConnection connection);
    protected static native KuzuQueryResult kuzu_connection_query(KuzuConnection connection, String query);
    protected static native KuzuPreparedStatement kuzu_connection_prepare(
        KuzuConnection connection, String query);
    protected static native KuzuQueryResult kuzu_connection_execute(
        KuzuConnection connection, KuzuPreparedStatement prepared_statement, Map<String, KuzuValue> param);
    protected static native String kuzu_connection_get_node_table_names(KuzuConnection connection);
    protected static native String kuzu_connection_get_rel_table_names(KuzuConnection connection);
    protected static native String kuzu_connection_get_node_property_names(
        KuzuConnection connection, String table_name);
    protected static native String kuzu_connection_get_rel_property_names(
        KuzuConnection connection, String table_name);
    protected static native void kuzu_connection_interrupt(KuzuConnection connection);
    protected static native void kuzu_connection_set_query_timeout(
        KuzuConnection connection, long timeout_in_ms);

    // PreparedStatement
    protected static native void kuzu_prepared_statement_destroy(KuzuPreparedStatement prepared_statement);
    protected static native boolean kuzu_prepared_statement_allow_active_transaction(
        KuzuPreparedStatement prepared_statement);
    protected static native boolean kuzu_prepared_statement_is_success(KuzuPreparedStatement prepared_statement);
    protected static native String kuzu_prepared_statement_get_error_message(
        KuzuPreparedStatement prepared_statement);
    /*
    protected static native void kuzu_prepared_statement_bind_bool(
        KuzuPreparedStatement prepared_statement, String param_name, boolean value);
    protected static native void kuzu_prepared_statement_bind_int64(
        KuzuPreparedStatement prepared_statement, String param_name, long value);
    protected static native void kuzu_prepared_statement_bind_int32(
        KuzuPreparedStatement prepared_statement, String param_name, int value);
    protected static native void kuzu_prepared_statement_bind_int16(
        KuzuPreparedStatement prepared_statement, String param_name, short value);
    protected static native void kuzu_prepared_statement_bind_double(
        KuzuPreparedStatement prepared_statement, String param_name, double value);
    protected static native void kuzu_prepared_statement_bind_float(
        KuzuPreparedStatement prepared_statement, String param_name, float value);
    protected static native void kuzu_prepared_statement_bind_date(
        KuzuPreparedStatement prepared_statement, String param_name, KuzuDate value);
    protected static native void kuzu_prepared_statement_bind_timestamp(
        KuzuPreparedStatement prepared_statement, String param_name, KuzuTimestamp value);
    protected static native void kuzu_prepared_statement_bind_interval(
        KuzuPreparedStatement prepared_statement, String param_name, KuzuInterval value);
    protected static native void kuzu_prepared_statement_bind_string(
        KuzuPreparedStatement prepared_statement, String param_name, String value);
    protected static native void kuzu_prepared_statement_bind_value(
        KuzuPreparedStatement prepared_statement, String param_name, KuzuValue value);
    */
    // QueryResult
    protected static native void kuzu_query_result_destroy(KuzuQueryResult query_result);
    protected static native boolean kuzu_query_result_is_success(KuzuQueryResult query_result);
    protected static native String kuzu_query_result_get_error_message(KuzuQueryResult query_result);
    protected static native long kuzu_query_result_get_num_columns(KuzuQueryResult query_result);
    protected static native String kuzu_query_result_get_column_name(KuzuQueryResult query_result, long index);
    protected static native KuzuDataType kuzu_query_result_get_column_data_type(
        KuzuQueryResult query_result, long index);
    protected static native long kuzu_query_result_get_num_tuples(KuzuQueryResult query_result);
    protected static native KuzuQuerySummary kuzu_query_result_get_query_summary(KuzuQueryResult query_result);
    protected static native boolean kuzu_query_result_has_next(KuzuQueryResult query_result);
    protected static native KuzuFlatTuple kuzu_query_result_get_next(KuzuQueryResult query_result);
    protected static native String kuzu_query_result_to_string(KuzuQueryResult query_result);
    protected static native void kuzu_query_result_write_to_csv(KuzuQueryResult query_result,
        String file_path, char delimiter, char escape_char, char new_line);
    protected static native void kuzu_query_result_reset_iterator(KuzuQueryResult query_result);

    // FlatTuple
    protected static native void kuzu_flat_tuple_destroy(KuzuFlatTuple flat_tuple);
    protected static native KuzuValue kuzu_flat_tuple_get_value(KuzuFlatTuple flat_tuple, long index);
    protected static native String kuzu_flat_tuple_to_string(KuzuFlatTuple flat_tuple);

    // DataType
    protected static native long kuzu_data_type_create(
        KuzuDataTypeID id, KuzuDataType child_type, long fixed_num_elements_in_list);
    protected static native KuzuDataType kuzu_data_type_clone(KuzuDataType data_type);
    protected static native void kuzu_data_type_destroy(KuzuDataType data_type);
    protected static native boolean kuzu_data_type_equals(KuzuDataType data_type1, KuzuDataType data_type2);
    protected static native KuzuDataTypeID kuzu_data_type_get_id(KuzuDataType data_type);
    protected static native KuzuDataType kuzu_data_type_get_child_type(KuzuDataType data_type);
    protected static native long kuzu_data_type_get_fixed_num_elements_in_list(KuzuDataType data_type);

    // Value
    protected static native KuzuValue kuzu_value_create_null();
    protected static native KuzuValue kuzu_value_create_null_with_data_type(KuzuDataType data_type);
    protected static native boolean kuzu_value_is_null(KuzuValue value);
    protected static native void kuzu_value_set_null(KuzuValue value, boolean is_null);
    protected static native KuzuValue kuzu_value_create_default(KuzuDataType data_type);
    protected static native KuzuValue kuzu_value_create_bool(boolean val_);
    protected static native KuzuValue kuzu_value_create_int16(short val_);
    protected static native KuzuValue kuzu_value_create_int32(int val_);
    protected static native KuzuValue kuzu_value_create_int64(long val_);
    protected static native KuzuValue kuzu_value_create_float(float val_);
    protected static native KuzuValue kuzu_value_create_double(double val_);
    protected static native KuzuValue kuzu_value_create_internal_id(KuzuInternalID val_);
    protected static native KuzuValue kuzu_value_create_node_val(KuzuNodeValue val_);
    protected static native KuzuValue kuzu_value_create_rel_val(KuzuRelValue val_);
    protected static native KuzuValue kuzu_value_create_date(KuzuDate val_);
    protected static native KuzuValue kuzu_value_create_timestamp(KuzuTimestamp val_);
    protected static native KuzuValue kuzu_value_create_interval(KuzuInterval val_);
    protected static native KuzuValue kuzu_value_create_string(String val_);
    protected static native KuzuValue kuzu_value_clone(KuzuValue value);
    protected static native void kuzu_value_copy(KuzuValue value, KuzuValue other);
    protected static native void kuzu_value_destroy(KuzuValue value);
    protected static native long kuzu_value_get_list_size(KuzuValue value);
    protected static native KuzuValue kuzu_value_get_list_element(KuzuValue value, long index);
    protected static native KuzuDataType kuzu_value_get_data_type(KuzuValue value);
    protected static native boolean kuzu_value_get_bool(KuzuValue value);
    protected static native short kuzu_value_get_int16(KuzuValue value);
    protected static native int kuzu_value_get_int32(KuzuValue value);
    protected static native long kuzu_value_get_int64(KuzuValue value);
    protected static native float kuzu_value_get_float(KuzuValue value);
    protected static native double kuzu_value_get_double(KuzuValue value);
    protected static native KuzuInternalID kuzu_value_get_internal_id(KuzuValue value);
    protected static native KuzuNodeValue kuzu_value_get_node_val(KuzuValue value);
    protected static native KuzuRelValue kuzu_value_get_rel_val(KuzuValue value);
    protected static native KuzuDate kuzu_value_get_date(KuzuValue value);
    protected static native KuzuTimestamp kuzu_value_get_timestamp(KuzuValue value);
    protected static native KuzuInterval kuzu_value_get_interval(KuzuValue value);
    protected static native String kuzu_value_get_string(KuzuValue value);
    protected static native String kuzu_value_to_string(KuzuValue value);

    protected static native KuzuNodeValue kuzu_node_val_create(KuzuInternalID id, String label);
    protected static native KuzuNodeValue kuzu_node_val_clone(KuzuNodeValue node_val);
    protected static native void kuzu_node_val_destroy(KuzuNodeValue node_val);
    protected static native KuzuValue kuzu_node_val_get_id_val(KuzuNodeValue node_val);
    protected static native KuzuValue kuzu_node_val_get_label_val(KuzuNodeValue node_val);
    protected static native KuzuInternalID kuzu_node_val_get_id(KuzuNodeValue node_val);
    protected static native String kuzu_node_val_get_label_name(KuzuNodeValue node_val);
    protected static native long kuzu_node_val_get_property_size(KuzuNodeValue node_val);
    protected static native String kuzu_node_val_get_property_name_at(KuzuNodeValue node_val, long index);
    protected static native KuzuValue kuzu_node_val_get_property_value_at(KuzuNodeValue node_val, long index);
    protected static native void kuzu_node_val_add_property(
        KuzuNodeValue node_val, String key, KuzuValue value);
    protected static native String kuzu_node_val_to_string(KuzuNodeValue node_val);

    protected static native KuzuRelValue kuzu_rel_val_create(
        KuzuInternalID src_id, KuzuInternalID dst_id, String label);
    protected static native KuzuRelValue kuzu_rel_val_clone(KuzuRelValue rel_val);
    protected static native void kuzu_rel_val_destroy(KuzuRelValue rel_val);
    protected static native KuzuValue kuzu_rel_val_get_src_id_val(KuzuRelValue rel_val);
    protected static native KuzuValue kuzu_rel_val_get_dst_id_val(KuzuRelValue rel_val);
    protected static native KuzuInternalID kuzu_rel_val_get_src_id(KuzuRelValue rel_val);
    protected static native KuzuInternalID kuzu_rel_val_get_dst_id(KuzuRelValue rel_val);
    protected static native String kuzu_rel_val_get_label_name(KuzuRelValue rel_val);
    protected static native long kuzu_rel_val_get_property_size(KuzuRelValue rel_val);
    protected static native String kuzu_rel_val_get_property_name_at(KuzuRelValue rel_val, long index);
    protected static native KuzuValue kuzu_rel_val_get_property_value_at(KuzuRelValue rel_val, long index);
    protected static native void kuzu_rel_val_add_property(KuzuRelValue rel_val, String key, KuzuValue value);
    protected static native String kuzu_rel_val_to_string(KuzuRelValue rel_val);

    // QuerySummary
    protected static native void kuzu_query_summary_destroy(KuzuQuerySummary query_summary);
    protected static native double kuzu_query_summary_get_compiling_time(KuzuQuerySummary query_summary);
    protected static native double kuzu_query_summary_get_execution_time(KuzuQuerySummary query_summary);



}