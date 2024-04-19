//
// Created by hti on 10/5/23.
//
#pragma once
#include "data_tables.h"

void generate_cache(const std::string& filepath, const std::string& sf, std::string& cache);
void generate_cache_win(const std::string& filepath, const std::string& sf, std::string& cache);
std::string round_to_one_decimal_and_convert_to_string(const float& value);
void data_read_from_storage(const std::string& tablepath, int& sf, float& zf, tpc_h_data_struct& tpc_h_data_struct) ;

//int get_row_count(const std::string& filename);


#ifndef SNIPPET_TEST_DATA_READ_H
#define SNIPPET_TEST_DATA_READ_H

#endif //SNIPPET_TEST_DATA_READ_H
