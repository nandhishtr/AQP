//
// Created by hti on 10/5/23.
//
#include <iostream>

#include <vector>
#include <string>
#include <fstream>
#include <chrono>
#include <sstream>
#include <stdexcept>

#include <cmath>
#include <filesystem>
#include <thread>
#include "data_tables.h"
#include "functions.h"
#include <iomanip>
#include <filesystem>
using namespace std;

int get_row_count(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    int rowCount = 0;
    std::string line;
    while (std::getline(file, line)) {
        rowCount++;
    }

    file.close();
    return rowCount;
}

std::string round_to_one_decimal_and_convert_to_string(const float& value) {
    std::string roundedString;
    // Round the float value to two decimal places
    float roundedValue = std::round(value * 10) / 10;

    // Convert the rounded value to a string with two digits after the decimal point
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1) << roundedValue;
    roundedString = ss.str();

    return roundedString;
}
void parse_lineitem_tbl_1_thread(const std::string& filename, sample_struct_lineitem& data, int sf) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    int progress_inc = 6000000*sf/100;
    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;
        if (line_num % progress_inc == 0) {
            std::cout << "Cache creation progress: " << line_num/progress_inc << "%" << std::endl;
        }
        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_lineitem
            if (columnCount == 0)
                data.l_orderkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.l_partkey.push_back(std::stoi(token));
            else if (columnCount == 2)
                data.l_suppkey.push_back(std::stoi(token));
            else if (columnCount == 3)
                data.l_linenumber.push_back(std::stoi(token));
            else if (columnCount == 4)
                data.l_quantity.push_back(std::stof(token));
            else if (columnCount == 5)
                data.l_extendedprice.push_back(std::stof(token));
            else if (columnCount == 6)
                data.l_discount.push_back(std::stof(token));
            else if (columnCount == 7)
                data.l_tax.push_back(std::stof(token));
            else if (columnCount == 8)
                data.l_returnflag.push_back(token[0]);
            else if (columnCount == 9)
                data.l_linestatus.push_back(token[0]);
            else if (columnCount == 10) {
                // Convert the date string to an integer
                std::string dateString = token;
                dateString.erase(std::remove(dateString.begin(), dateString.end(), '-'), dateString.end());
                int dateValue = std::stoi(dateString);                // Save the integer value in the l_shipdate vector
                data.l_shipdate.push_back(dateValue);
            }
            else if (columnCount == 11) {
                // Convert the date string to an integer
                std::string dateString = token;
                dateString.erase(std::remove(dateString.begin(), dateString.end(), '-'), dateString.end());
                int dateValue = std::stoi(dateString);
                data.l_commidate.push_back(dateValue);
            }
            else if (columnCount == 12) {
                // Convert the date string to an integer
                std::string dateString = token;
                dateString.erase(std::remove(dateString.begin(), dateString.end(), '-'), dateString.end());
                int dateValue = std::stoi(dateString);
                data.l_receiptdate.push_back(dateValue);
            }
            else if (columnCount == 13)
                data.l_shipinstruct.push_back(token);
            else if (columnCount == 14)
                data.l_shipmode.push_back(token);
            else if (columnCount == 15)
                data.l_comment.push_back(token);
            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_part_tbl_1_thread(const std::string& filename, sample_struct_part& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_part
            if (columnCount == 0)
                data.p_partkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.p_name.push_back(token);
            else if (columnCount == 2)
                data.p_mfgr.push_back(token);
            else if (columnCount == 3)
                data.p_brand.push_back(token);
            else if (columnCount == 4)
                data.p_type.push_back(token);
            else if (columnCount == 5)
                data.p_size.push_back(std::stoi(token));
            else if (columnCount == 6)
                data.p_container.push_back(token);
            else if (columnCount == 7)
                data.p_retailprice.push_back(std::stof(token));
            else if (columnCount == 8)
                data.p_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_supplier_tbl_1_thread(const std::string& filename, sample_struct_supplier& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_supplier
            if (columnCount == 0)
                data.s_suppkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.s_name.push_back(token);
            else if (columnCount == 2)
                data.s_address.push_back(token);
            else if (columnCount == 3)
                data.s_nationkey.push_back(std::stoi(token));
            else if (columnCount == 4)
                data.s_phone.push_back(token);
            else if (columnCount == 5)
                data.s_acctbal.push_back(std::stoi(token));
            else if (columnCount == 6)
                data.s_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_partsupp_tbl_1_thread(const std::string& filename, sample_struct_partsupp& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_partsupp
            if (columnCount == 0)
                data.ps_partkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.ps_suppkey.push_back(std::stoi(token));
            else if (columnCount == 2)
                data.ps_availqty.push_back(std::stoi(token));
            else if (columnCount == 3)
                data.ps_supplycost.push_back(std::stof(token));
            else if (columnCount == 4)
                data.ps_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_customer_tbl_1_thread(const std::string& filename, sample_struct_customer& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_customer
            if (columnCount == 0)
                data.c_custkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.c_name.push_back(token);
            else if (columnCount == 2)
                data.c_address.push_back(token);
            else if (columnCount == 3)
                data.c_nationkey.push_back(std::stoi(token));
            else if (columnCount == 4)
                data.c_phone.push_back(token);
            else if (columnCount == 5)
                data.c_acctbal.push_back(std::stof(token));
            else if (columnCount == 6)
                data.c_mktsegment.push_back(token);
            else if (columnCount == 7)
                data.c_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_orders_tbl_1_thread(const std::string& filename, sample_struct_orders& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_orders
            if (columnCount == 0)
                data.o_orderkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.o_custkey.push_back(std::stoi(token));
            else if (columnCount == 2)
                data.o_orderstatus.push_back(token[0]);
            else if (columnCount == 3)
                data.o_totalprice.push_back(std::stof(token));
            else if (columnCount == 4) {
                // Convert the date string to an integer
                std::string dateString = token;
                dateString.erase(std::remove(dateString.begin(), dateString.end(), '-'), dateString.end());
                int dateValue = std::stoi(dateString);                // Save the integer value in the l_shipdate vector
                data.o_orderdate.push_back(dateValue);
            }
            else if (columnCount == 5)
                data.o_orderpriority.push_back(token);
            else if (columnCount == 6)
                data.o_clerk.push_back(token);
            else if (columnCount == 7)
                data.o_shippriority.push_back(std::stoi(token));
            else if (columnCount == 8)
                data.o_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_nation_tbl_1_thread(const std::string& filename, sample_struct_nation& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_nation
            if (columnCount == 0)
                data.n_nationkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.n_name.push_back(token);
            else if (columnCount == 2)
                data.n_regionkey.push_back(std::stoi(token));
            else if (columnCount == 3)
                data.n_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}

void parse_region_tbl_1_thread(const std::string& filename, sample_struct_region& data) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    int line_num = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        int columnCount = 0;
        line_num++;

        while (std::getline(ss, token, '|')) {
            // Save the token in the appropriate vector of the sample_struct_region
            if (columnCount == 0)
                data.r_regionkey.push_back(std::stoi(token));
            else if (columnCount == 1)
                data.r_name.push_back(token);
            else if (columnCount == 2)
                data.r_comment.push_back(token);

            // Increment the column count
            columnCount++;
        }
    }

    file.close();
}


void write_column_to_file(const std::vector<int>& column, const std::string& filename) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }
    file.write(reinterpret_cast<const char*>(column.data()), std::streamsize(column.size() * sizeof(int)));
    file.close();
}

void write_column_to_file(const std::vector<float>& column, const std::string& filename) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }
    file.write(reinterpret_cast<const char*>(column.data()), std::streamsize(column.size() * sizeof(float)));
    file.close();
}

void write_column_to_file(const std::vector<char>& column, const std::string& filename) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }
    file.write(reinterpret_cast<const char*>(column.data()), std::streamsize(column.size() * sizeof(char)));
    file.close();
}


void write_column_to_file(const std::vector<std::string>& column, const std::string& filename) {
    std::ofstream file(filename);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }
    for (const auto& value : column) {
        file << value << "\n";
    }
    file.close();
}
void write_lineitem_table_to_cache (sample_struct_lineitem & data, std::string & cache){

    write_column_to_file(data.l_orderkey, cache + "l_orderkey.bin");
    write_column_to_file(data.l_partkey, cache + "l_partkey.bin");
    write_column_to_file(data.l_suppkey, cache + "l_suppkey.bin");
    write_column_to_file(data.l_linenumber, cache + "l_linenumber.bin");
    write_column_to_file(data.l_quantity, cache + "l_quantity.bin");
    write_column_to_file(data.l_extendedprice, cache + "l_extendedprice.bin");
    write_column_to_file(data.l_discount, cache + "l_discount.bin");
    write_column_to_file(data.l_tax, cache + "l_tax.bin");
    write_column_to_file(data.l_returnflag, cache + "l_returnflag.bin");
    write_column_to_file(data.l_linestatus, cache + "l_linestatus.bin");
    write_column_to_file(data.l_shipdate, cache + "l_shipdate.bin");
    write_column_to_file(data.l_commidate, cache + "l_commidate.bin");
    write_column_to_file(data.l_receiptdate, cache + "l_receiptdate.bin");
    write_column_to_file(data.l_shipinstruct, cache + "l_shipinstruct.txt");
    write_column_to_file(data.l_shipmode, cache + "l_shipmode.txt");
    write_column_to_file(data.l_comment, cache + "l_comment.txt");
    data.clearData();
}

void write_part_table_to_cache(sample_struct_part& data, std::string& cache) {
    write_column_to_file(data.p_partkey, cache + "p_partkey.bin");
    write_column_to_file(data.p_name, cache + "p_name.txt");
    write_column_to_file(data.p_mfgr, cache + "p_mfgr.txt");
    write_column_to_file(data.p_brand, cache + "p_brand.txt");
    write_column_to_file(data.p_type, cache + "p_type.txt");
    write_column_to_file(data.p_size, cache + "p_size.bin");
    write_column_to_file(data.p_container, cache + "p_container.txt");
    write_column_to_file(data.p_retailprice, cache + "p_retailprice.bin");
    write_column_to_file(data.p_comment, cache + "p_comment.txt");
    data.clearData();
}

void write_supplier_table_to_cache(sample_struct_supplier& data, std::string& cache) {
    write_column_to_file(data.s_suppkey, cache + "s_suppkey.bin");
    write_column_to_file(data.s_name, cache + "s_name.txt");
    write_column_to_file(data.s_address, cache + "s_address.txt");
    write_column_to_file(data.s_nationkey, cache + "s_nationkey.bin");
    write_column_to_file(data.s_phone, cache + "s_phone.txt");
    write_column_to_file(data.s_acctbal, cache + "s_acctbal.bin");
    write_column_to_file(data.s_comment, cache + "s_comment.txt");
    data.clearData();
}
void write_partsupp_table_to_cache(sample_struct_partsupp& data, std::string& cache) {
    write_column_to_file(data.ps_partkey, cache + "ps_partkey.bin");
    write_column_to_file(data.ps_suppkey, cache + "ps_suppkey.bin");
    write_column_to_file(data.ps_availqty, cache + "ps_availqty.bin");
    write_column_to_file(data.ps_supplycost, cache + "ps_supplycost.bin");
    write_column_to_file(data.ps_comment, cache + "ps_comment.txt");
    data.clearData();
}

void write_customer_table_to_cache(sample_struct_customer& data, std::string& cache) {
    write_column_to_file(data.c_custkey, cache + "c_custkey.bin");
    write_column_to_file(data.c_name, cache + "c_name.txt");
    write_column_to_file(data.c_address, cache + "c_address.txt");
    write_column_to_file(data.c_nationkey, cache + "c_nationkey.bin");
    write_column_to_file(data.c_phone, cache + "c_phone.txt");
    write_column_to_file(data.c_acctbal, cache + "c_acctbal.bin");
    write_column_to_file(data.c_mktsegment, cache + "c_mktsegment.txt");
    write_column_to_file(data.c_comment, cache + "c_comment.txt");
    data.clearData();
}

void write_orders_table_to_cache(sample_struct_orders& data, std::string& cache) {
    write_column_to_file(data.o_orderkey, cache + "o_orderkey.bin");
    write_column_to_file(data.o_custkey, cache + "o_custkey.bin");
    write_column_to_file(data.o_orderstatus, cache + "o_orderstatus.bin");
    write_column_to_file(data.o_totalprice, cache + "o_totalprice.bin");
    write_column_to_file(data.o_orderdate, cache + "o_orderdate.bin");
    write_column_to_file(data.o_orderpriority, cache + "o_orderpriority.txt");
    write_column_to_file(data.o_clerk, cache + "o_clerk.txt");
    write_column_to_file(data.o_shippriority, cache + "o_shippriority.bin");
    write_column_to_file(data.o_comment, cache + "o_comment.txt");
    data.clearData();
}

void write_nation_table_to_cache(sample_struct_nation& data, std::string& cache) {
    write_column_to_file(data.n_nationkey, cache + "n_nationkey.bin");
    write_column_to_file(data.n_name, cache + "n_name.txt");
    write_column_to_file(data.n_regionkey, cache + "n_regionkey.bin");
    write_column_to_file(data.n_comment, cache + "n_comment.txt");
    data.clearData();
}

void write_region_table_to_cache(sample_struct_region& data, std::string& cache) {
    write_column_to_file(data.r_regionkey, cache + "r_regionkey.bin");
    write_column_to_file(data.r_name, cache + "r_name.txt");
    write_column_to_file(data.r_comment, cache + "r_comment.txt");
    data.clearData();
}

bool is_directory_empty(const std::string& path) {
    //if (!std::filesystem::exists(path)) {
    //    // The path doesn't exist
    //    return true;
    //}

    //if (!std::filesystem::is_directory(path)) {
    //    // The path is not a directory
    //    return false;
    //}

    //return std::filesystem::is_empty(path);
    return true;
}




std::uintmax_t get_file_size(const std::string& filePath) {
    //try {
    //    // Check if the file exists
    //    if (std::filesystem::exists(filePath)) {
    //        // Get the file size in bytes
    //        return std::filesystem::file_size(filePath);
    //    } else {
    //        std::cerr << "File not found: " << filePath << std::endl;
    //    }
    //} catch (const std::exception& e) {
    //    std::cerr << "Error occurred: " << e.what() << std::endl;
    //}

    //// Return 0 if there was an error or the file doesn't exist
    return 0;
}

void generate_cache_win(const std::string& filepath, const std::string& sf, std::string& cache) {
    // Get the row count from the file
    //int rows_for_cache = get_row_count(filepath);
    const std::string customer_tbl = "\\customer.tbl";
    const std::string lineitem_tbl = "\\lineitem.tbl";
    const std::string nation_tbl   = "\\nation.tbl";
    const std::string order_tbl    = "\\order.tbl";
    const std::string part_tbl     = "\\part.tbl";
    const std::string partsupp_tbl = "\\partsupp.tbl";
    const std::string region_tbl   = "\\region.tbl";
    const std::string supplier_tbl = "\\supplier.tbl";
    // Create a data structure to hold tables data for caching
    sample_struct_customer customer_data_to_cache(0);
    sample_struct_lineitem lineitem_data_to_cache(0);
    sample_struct_nation nation_data_to_cache (0);
    sample_struct_orders orders_data_to_cache(0);
    sample_struct_part part_data_to_cache(0);
    sample_struct_partsupp partsupp_data_to_cache(0);
    sample_struct_region region_data_to_cache(0);
    sample_struct_supplier supplier_data_to_cache(0);

    std::cout << "There is no cache. Generating." << std::endl;
    auto tc0 = std::chrono::high_resolution_clock::now();

//Time to read, parse and write cached data to storage
    parse_customer_tbl_1_thread(filepath+customer_tbl, customer_data_to_cache);
    write_customer_table_to_cache(customer_data_to_cache, cache);

    parse_nation_tbl_1_thread(filepath+nation_tbl, nation_data_to_cache);
    write_nation_table_to_cache(nation_data_to_cache, cache);

    parse_orders_tbl_1_thread(filepath + order_tbl, orders_data_to_cache);
    write_orders_table_to_cache(orders_data_to_cache, cache);

    parse_part_tbl_1_thread(filepath + part_tbl, part_data_to_cache);
    write_part_table_to_cache(part_data_to_cache, cache);

    parse_partsupp_tbl_1_thread(filepath + partsupp_tbl, partsupp_data_to_cache);
    write_partsupp_table_to_cache(partsupp_data_to_cache, cache);

    parse_region_tbl_1_thread(filepath + region_tbl, region_data_to_cache);
    write_region_table_to_cache(region_data_to_cache, cache);

    parse_supplier_tbl_1_thread(filepath + supplier_tbl, supplier_data_to_cache);
    write_supplier_table_to_cache(supplier_data_to_cache, cache);

    parse_lineitem_tbl_1_thread(filepath+lineitem_tbl, lineitem_data_to_cache, std::stoi(sf));
    write_lineitem_table_to_cache(lineitem_data_to_cache, cache);


    auto tc1 = std::chrono::high_resolution_clock::now();
    auto caching_time = std::chrono::duration_cast<std::chrono::microseconds>(tc1 - tc0);
    std::cout << "Cache generation took : " << static_cast<float>(caching_time.count()) / 1000 << " ms " << std::endl;
}


void generate_cache(const std::string& filepath, const std::string& sf, std::string& cache) {
    // Get the row count from the file
    //int rows_for_cache = get_row_count(filepath);
    const std::string customer_tbl = "/customer.tbl";
    const std::string lineitem_tbl = "/lineitem.tbl";
    const std::string nation_tbl = "/nation.tbl";
    const std::string order_tbl = "/order.tbl";
    const std::string part_tbl = "/part.tbl";
    const std::string partsupp_tbl = "/partsupp.tbl";
    const std::string region_tbl = "/region.tbl";
    const std::string supplier_tbl = "/supplier.tbl";
    // Create a data structure to hold tables data for caching
    sample_struct_customer customer_data_to_cache(0);
    sample_struct_lineitem lineitem_data_to_cache(0);
    sample_struct_nation nation_data_to_cache (0);
    sample_struct_orders orders_data_to_cache(0);
    sample_struct_part part_data_to_cache(0);
    sample_struct_partsupp partsupp_data_to_cache(0);
    sample_struct_region region_data_to_cache(0);
    sample_struct_supplier supplier_data_to_cache(0);

    std::cout << "There is no cache. Generating." << std::endl;
    auto tc0 = std::chrono::high_resolution_clock::now();

//Time to read, parse and write cached data to storage
    parse_customer_tbl_1_thread(filepath+customer_tbl, customer_data_to_cache);
    write_customer_table_to_cache(customer_data_to_cache, cache);

    parse_nation_tbl_1_thread(filepath+nation_tbl, nation_data_to_cache);
    write_nation_table_to_cache(nation_data_to_cache, cache);

    parse_orders_tbl_1_thread(filepath + order_tbl, orders_data_to_cache);
    write_orders_table_to_cache(orders_data_to_cache, cache);

    parse_part_tbl_1_thread(filepath + part_tbl, part_data_to_cache);
    write_part_table_to_cache(part_data_to_cache, cache);

    parse_partsupp_tbl_1_thread(filepath + partsupp_tbl, partsupp_data_to_cache);
    write_partsupp_table_to_cache(partsupp_data_to_cache, cache);

    parse_region_tbl_1_thread(filepath + region_tbl, region_data_to_cache);
    write_region_table_to_cache(region_data_to_cache, cache);

    parse_supplier_tbl_1_thread(filepath + supplier_tbl, supplier_data_to_cache);
    write_supplier_table_to_cache(supplier_data_to_cache, cache);

    parse_lineitem_tbl_1_thread(filepath+lineitem_tbl, lineitem_data_to_cache, std::stoi(sf));
    write_lineitem_table_to_cache(lineitem_data_to_cache, cache);


    auto tc1 = std::chrono::high_resolution_clock::now();
    auto caching_time = std::chrono::duration_cast<std::chrono::microseconds>(tc1 - tc0);
    std::cout << "Cache generation took : " << static_cast<float>(caching_time.count()) / 1000 << " ms " << std::endl;
}


class FileReader {
public:
    static void read_vector_from_file(const std::string& filePath, std::vector<int>& vector) {
        std::ifstream file(filePath, std::ios::binary);
        if (file) {
            int value;
            while (file.read(reinterpret_cast<char*>(&value), sizeof(int))) {
                vector.push_back(value);
            }
            file.close();
        } else {
            std::cerr << "Failed to open file: " << filePath << std::endl;
        }
    }

    static void read_vector_from_file(const std::string& filePath, std::vector<float>& vector) {
        std::ifstream file(filePath, std::ios::binary);
        if (file) {
            float value;
            while (file.read(reinterpret_cast<char*>(&value), sizeof(float))) {
                vector.push_back(value);
            }
            file.close();
        } else {
            std::cerr << "Failed to open file: " << filePath << std::endl;
        }
    }

    static void read_vector_from_file(const std::string& filePath, std::vector<char>& vector) {
        std::ifstream file(filePath);
        if (file) {
            std::string line;
            if (std::getline(file, line)) {
                for (char c : line) {
                    vector.push_back(c);
                }
            }
            file.close();
        } else {
            std::cerr << "Failed to open file: " << filePath << std::endl;
        }
    }

    static void read_vector_from_file(const std::string& filePath, std::vector<std::string>& vector) {
        std::ifstream file(filePath);
        if (file) {
            std::string value;
            while (std::getline(file, value)) {
                vector.push_back(value);
            }
            file.close();
        } else {
            std::cerr << "Failed to open file: " << filePath << std::endl;
        }
    }
};

void read_lineitem_to_sample_struct(const std::string& cache_path, sample_struct_lineitem& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "/l_orderkey.bin", std::ref(data.l_orderkey));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_partkey.bin",  std::ref(data.l_partkey));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_suppkey.bin",  std::ref(data.l_suppkey));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_linenumber.bin",  std::ref(data.l_linenumber));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_quantity.bin",  std::ref(data.l_quantity));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_extendedprice.bin",  std::ref(data.l_extendedprice));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_discount.bin",  std::ref(data.l_discount));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_tax.bin",  std::ref(data.l_tax));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_returnflag.bin",  std::ref(data.l_returnflag));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_linestatus.bin",  std::ref(data.l_linestatus));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_shipdate.bin",  std::ref(data.l_shipdate));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_commidate.bin",  std::ref(data.l_commidate));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_receiptdate.bin",  std::ref(data.l_receiptdate));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_shipinstruct.txt",  std::ref(data.l_shipinstruct));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_shipmode.txt",  std::ref(data.l_shipmode));
    threads.emplace_back(read_file_in_thread, cache_path + "/l_comment.txt",  std::ref(data.l_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_part_to_sample_struct(const std::string& cache_path, sample_struct_part& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "p_partkey.bin", std::ref(data.p_partkey));
    threads.emplace_back(read_file_in_thread, cache_path + "p_name.txt",  std::ref(data.p_name));
    threads.emplace_back(read_file_in_thread, cache_path + "p_mfgr.txt",  std::ref(data.p_mfgr));
    threads.emplace_back(read_file_in_thread, cache_path + "p_brand.txt",  std::ref(data.p_brand));
    threads.emplace_back(read_file_in_thread, cache_path + "p_type.txt",  std::ref(data.p_type));
    threads.emplace_back(read_file_in_thread, cache_path + "p_size.bin",  std::ref(data.p_size));
    threads.emplace_back(read_file_in_thread, cache_path + "p_container.txt",  std::ref(data.p_container));
    threads.emplace_back(read_file_in_thread, cache_path + "p_retailprice.bin",  std::ref(data.p_retailprice));
    threads.emplace_back(read_file_in_thread, cache_path + "p_comment.txt",  std::ref(data.p_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_supplier_to_sample_struct(const std::string& cache_path, sample_struct_supplier& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "s_suppkey.bin", std::ref(data.s_suppkey));
    threads.emplace_back(read_file_in_thread, cache_path + "s_name.txt",  std::ref(data.s_name));
    threads.emplace_back(read_file_in_thread, cache_path + "s_address.txt",  std::ref(data.s_address));
    threads.emplace_back(read_file_in_thread, cache_path + "s_nationkey.bin",  std::ref(data.s_nationkey));
    threads.emplace_back(read_file_in_thread, cache_path + "s_phone.txt",  std::ref(data.s_phone));
    threads.emplace_back(read_file_in_thread, cache_path + "s_acctbal.bin",  std::ref(data.s_acctbal));
    threads.emplace_back(read_file_in_thread, cache_path + "s_comment.txt",  std::ref(data.s_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_partsupp_to_sample_struct(const std::string& cache_path, sample_struct_partsupp& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "ps_partkey.bin", std::ref(data.ps_partkey));
    threads.emplace_back(read_file_in_thread, cache_path + "ps_suppkey.bin",  std::ref(data.ps_suppkey));
    threads.emplace_back(read_file_in_thread, cache_path + "ps_availqty.bin",  std::ref(data.ps_availqty));
    threads.emplace_back(read_file_in_thread, cache_path + "ps_supplycost.bin",  std::ref(data.ps_supplycost));
    threads.emplace_back(read_file_in_thread, cache_path + "ps_comment.txt",  std::ref(data.ps_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_customer_to_sample_struct(const std::string& cache_path, sample_struct_customer& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "c_custkey.bin", std::ref(data.c_custkey));
    threads.emplace_back(read_file_in_thread, cache_path + "c_name.txt",  std::ref(data.c_name));
    threads.emplace_back(read_file_in_thread, cache_path + "c_address.txt",  std::ref(data.c_address));
    threads.emplace_back(read_file_in_thread, cache_path + "c_nationkey.bin",  std::ref(data.c_nationkey));
    threads.emplace_back(read_file_in_thread, cache_path + "c_phone.txt",  std::ref(data.c_phone));
    threads.emplace_back(read_file_in_thread, cache_path + "c_acctbal.bin",  std::ref(data.c_acctbal));
    threads.emplace_back(read_file_in_thread, cache_path + "c_mktsegment.txt",  std::ref(data.c_mktsegment));
    threads.emplace_back(read_file_in_thread, cache_path + "c_comment.txt",  std::ref(data.c_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_orders_to_sample_struct(const std::string& cache_path, sample_struct_orders& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "o_orderkey.bin", std::ref(data.o_orderkey));
    threads.emplace_back(read_file_in_thread, cache_path + "o_custkey.bin",  std::ref(data.o_custkey));
    threads.emplace_back(read_file_in_thread, cache_path + "o_orderstatus.bin",  std::ref(data.o_orderstatus));
    threads.emplace_back(read_file_in_thread, cache_path + "o_totalprice.bin",  std::ref(data.o_totalprice));
    threads.emplace_back(read_file_in_thread, cache_path + "o_orderdate.bin",  std::ref(data.o_orderdate));
    threads.emplace_back(read_file_in_thread, cache_path + "o_orderpriority.txt",  std::ref(data.o_orderpriority));
    threads.emplace_back(read_file_in_thread, cache_path + "o_clerk.txt",  std::ref(data.o_clerk));
    threads.emplace_back(read_file_in_thread, cache_path + "o_shippriority.bin",  std::ref(data.o_shippriority));
    threads.emplace_back(read_file_in_thread, cache_path + "o_comment.txt",  std::ref(data.o_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_nation_to_sample_struct(const std::string& cache_path, sample_struct_nation& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "n_nationkey.bin", std::ref(data.n_nationkey));
    threads.emplace_back(read_file_in_thread, cache_path + "n_name.txt",  std::ref(data.n_name));
    threads.emplace_back(read_file_in_thread, cache_path + "n_regionkey.bin",  std::ref(data.n_regionkey));
    threads.emplace_back(read_file_in_thread, cache_path + "n_comment.txt",  std::ref(data.n_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void read_region_to_sample_struct(const std::string& cache_path, sample_struct_region& data) {
    FileReader fileReader;

    // Function to read a file in a separate thread
    auto read_file_in_thread = [&fileReader](const std::string& filePath, auto vector) {
        fileReader.read_vector_from_file(filePath, vector);
    };

    // Create threads for each file read operation
    std::vector<std::thread> threads;
    threads.emplace_back(read_file_in_thread, cache_path + "r_regionkey.bin", std::ref(data.r_regionkey));
    threads.emplace_back(read_file_in_thread, cache_path + "r_name.txt",  std::ref(data.r_name));
    threads.emplace_back(read_file_in_thread, cache_path + "r_comment.txt",  std::ref(data.r_comment));

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
}


void data_read_from_storage(const std::string& tablepath, int& sf, float& zf, tpc_h_data_struct& tpc_h_data_struct) {
    tpc_h_data_struct.clearData();
    std::string zf_string;
    zf_string = round_to_one_decimal_and_convert_to_string(zf);
    //----windows run
    std::string filepath = tablepath + std::to_string(sf) + "\\zf" + zf_string ;
    //----linux run
    //std::string filepath = tablepath + std::to_string(sf) + "/zf" + zf_string ;
    //for (std::string zf: rounded_zf_string) {


    //std::string cache = tablepath + (sf) + "/zf" + zf + "/cache/"


    // Create the cache directory if it doesn't exist
    //----linux run
    //std::string cacheDirectory = filepath + "/cache/";
    //----windows run
    std::string cacheDirectory = filepath + "\\cache\\";
    create_directory_if_not_exists(cacheDirectory);

    // Check if the cache directory is empty
    if (is_directory_empty(cacheDirectory)) {
        // Generate cache if it's empty
        //----linux
        //generate_cache(filepath, std::to_string(sf), cacheDirectory);
        //----windows
        generate_cache_win(filepath, std::to_string(sf), cacheDirectory);


    } else {
        std::cout << "Cache is available. Reading cached data_lineitem." << std::endl;
    }

    std::uintmax_t customer_size;
    std::uintmax_t lineitem_size;
    std::uintmax_t nation_size;
    std::uintmax_t orders_size;
    std::uintmax_t part_size;
    std::uintmax_t partsupp_size;
    std::uintmax_t region_size;
    std::uintmax_t supplier_size;

    // Get the number of rows in table from the cache file
    customer_size = get_file_size(cacheDirectory + "c_custkey.bin")/4;
    lineitem_size = get_file_size(cacheDirectory + "l_returnflag.bin");
    nation_size = get_file_size(cacheDirectory + "n_nationkey.bin")/4;
    orders_size = get_file_size(cacheDirectory + "o_orderkey.bin")/4;
    part_size = get_file_size(cacheDirectory + "p_partkey.bin")/4;
    partsupp_size = get_file_size(cacheDirectory + "ps_partkey.bin")/4;
    region_size = get_file_size(cacheDirectory + "r_regionkey.bin")/4;
    supplier_size = get_file_size(cacheDirectory + "s_suppkey.bin")/4;
    // Initialize data_lineitem.id with zeros
    //data_lineitem.id.resize(lineitem_size);

    tpc_h_data_struct.lineitem_table.id.assign(lineitem_size, 0);
    tpc_h_data_struct.customer_table.id.assign(customer_size, 0);
    tpc_h_data_struct.nation_table.id.assign(nation_size, 0);
    tpc_h_data_struct.orders_table.id.assign(orders_size, 0);
    tpc_h_data_struct.part_table.id.assign(part_size, 0);
    tpc_h_data_struct.partsupp_table.id.assign(partsupp_size, 0);
    tpc_h_data_struct.region_table.id.assign(region_size, 0);
    tpc_h_data_struct.supplier_table.id.assign(supplier_size, 0);


    // Record the start time
    auto t1 = std::chrono::high_resolution_clock::now();

    // Read files into data_lineitem
    read_lineitem_to_sample_struct(cacheDirectory, tpc_h_data_struct.lineitem_table);
    read_customer_to_sample_struct(cacheDirectory, tpc_h_data_struct.customer_table);
    read_nation_to_sample_struct(cacheDirectory, tpc_h_data_struct.nation_table);
    read_orders_to_sample_struct(cacheDirectory, tpc_h_data_struct.orders_table);
    read_part_to_sample_struct(cacheDirectory, tpc_h_data_struct.part_table);
    read_partsupp_to_sample_struct(cacheDirectory, tpc_h_data_struct.partsupp_table);
    read_region_to_sample_struct(cacheDirectory, tpc_h_data_struct.region_table);
    read_supplier_to_sample_struct(cacheDirectory, tpc_h_data_struct.supplier_table);

}
