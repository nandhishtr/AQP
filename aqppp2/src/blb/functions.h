//
// Created by burtsev on 12/9/22.
//

#pragma once
#include <iostream>
#include <cstdlib>
#include<cstring>
#include <chrono>
#include <random>
#include <fstream>
#include <string>
#include <vector>
#include <cmath>
#include <omp.h>
#include <string>
#include <map>

#ifndef SAMPLE_STRUCT_H
#define SAMPLE_STRUCT_H

#include <vector>
#include <string>
#include "data_tables.h"
#include "data_read.h"

#include "..\exp\comprehensive_exp.h"

#endif  // SAMPLE_STRUCT_H

//
//struct iParser {
//    virtual void consider(const char *) = 0;
//};
//
//template<typename T> struct Traits;

//template<typename T>
//struct AppendingParser : public iParser{
//    std::vector<T> & dest;
//    explicit AppendingParser(std::vector<T> & dest_) : dest(dest_) {}
//    void consider(const char * v) override {
//        dest.push_back(Traits<T>::parse(v));
//    }
//};
//
//template<>
//struct Traits<int> {
//    static int parse(const char * s) { return atoi(s); }
//};
//
//template<>
//struct Traits<float> {
//    static float parse(const char * s) { return atof(s); }
//};
//
//template<>
//struct Traits<char> {
//    static float parse(const char * s) { return atof(s); }
//};
//
//template<>
//struct Traits<std::string> {
//    static std::string parse(const char * s) { return s; }
//};
void show_available_commands();
std::vector<std::string> round_to_one_decimal_and_convert_to_string(const std::vector<float>& values);
std::vector<std::vector<std::string>> output_file_rows_names_write ();
void subsample_rows(const sample_struct_lineitem& data, std::size_t numSamples, sample_struct_lineitem& subsampled_data );

void write_vector_to_file(const std::vector<int>& data, const std::string& filename);
std::size_t count_agg   (std::vector<float> data);
float average_agg       (std::vector<float> data);
double sum_agg_vec(const std::vector<float>& data);

bool create_directory_if_not_exists(const std::string& directoryPath);
std::uintmax_t get_file_size(const std::string& filePath);
double calculate_standard_error(const std::vector<double>& values);
std::pair<double, double> calculate_confidence_interval(const std::vector<double>& values, double confidenceLevel);
void remove_zeros_from_vec(std::vector<float>& vec);
double calculate_mean(const std::vector<double>& values);
void write_2d_vector_to_csv(const std::vector<std::vector<std::string>>& data, const std::string& filename);

std::map<float, int> calculate_frequency(const std::vector<float>& values);
std::map<int, int> calculate_frequency(const std::vector<int>& values);
std::map<char, int> calculate_frequency(const std::vector<char>& values);
std::map<std::string, int> calculate_frequency(const std::vector<std::string>& values);

void write_frequency_table_to_CSV(const std::map<int, int>&   frequencyMap, const std::string& outputFilepath, const std::string& filename);
void write_frequency_table_to_CSV(const std::map<float, int>& frequencyMap, const std::string& outputFilepath, const std::string& filename);
void write_frequency_table_to_CSV(const std::map<char, int>&  frequencyMap, const std::string& outputFilepath, const std::string& filename);
void write_frequency_table_to_CSV(const std::map<std::string, int>& frequencyMap, const std::string& outputFilepath, const std::string& filename);
void write_data_frequencies_to_csv(sample_struct_lineitem const &data, const std::string& zf);



sample_struct_lineitem subsample_rows_w_return(const sample_struct_lineitem& data, std::size_t numSamples );


void aqpp_blb_filtering(std::vector <std::vector<double>> sample, std::vector<aqppp::Condition> cur_q, sample_struct_lineitem& query_data);
void multi_threaded_filtering(std::vector<sample_struct_lineitem>& BLB_subsample_data_vec, std::vector <std::vector <std::vector<double>>>& aqpp_blb_sample, std::vector<aqppp::Condition>& cur_q);

