    // Serialize a given DataFrame object
    // Serialized message will take the form:
    // "[Serialized Schema];;[Serialized Column 0]; ... ;[Serialized Column n-1]"
    virtual char* serialize_dataframe(DataFrame* df) {
        // Track total buffer size we need
        size_t total_str_size = 0;
        
        // Serialize schema
        char* schema_str = serialize_schema(&df->get_schema());
        total_str_size += strlen(schema_str);

        // Serialize all columns
        size_t cols = df->ncols();
        char** col_strs = new char*[cols];
        for (size_t i = 0; i < cols; i++) {
            // Serialize column based on schema 
            col_strs[i] = serialize_col(df->get_col_(i), df->get_schema().col_type(i));
            total_str_size += strlen(col_strs[i]);
        }

        char* serial_buffer = new char[total_str_size];

        // Copy schema and column strings into buffer
        memcpy(serial_buffer, schema_str, strlen(schema_str));
        strcat(serial_buffer, ";;");
        
        // Add serialized column messages to buffer, delimeted by ";"
        for (size_t j = 0; j < cols; j++) {
            strcat(serial_buffer, col_strs[j]);
            // Do not append ";" after last column string
            if (j != cols - 1) {
                strcat(serial_buffer, ";");
            }
            delete[] col_strs[j];
        } 
        delete[] col_strs;
        return serial_buffer;
    }

    // Deserialize a char* buffer into a DataFrame object
    virtual DataFrame* deserialize_dataframe(char* msg) {
        // Tokenize message
        char* token = strtok(msg, ";;");
        
        Schema* schema = deserialize_schema(token);

        Schema empty_schema();

        // Initialize
        DataFrame* df = new DataFrame(empty_schema);

        // Deserialize each column and add it to the dataframe
        for (size_t i = 0; i < schema->width(); i++) {
            token = strtok(nullptr, ";");
            char type = schema->col_type(i);
            if (type == INT_TYPE) {
                df->add_column(deserialize_int_col(token), nullptr);
            } else if (type == BOOL_TYPE) {
                df->add_column(deserialize_bool_col(token), nullptr);
            } else if (type == FLOAT_TYPE) {
                df->add_column(deserialize_float_col(token), nullptr);
            } else {
                df->add_column(deserialize_string_col(token), nullptr);
            }
        }

        delete schema;
        return df;
    }
