//
// Created by hti on 10/6/23.
//
#include "data_tables.h"
#include "data_read.h"

#ifndef TPC_H_Q6_HPP
#define TPC_H_Q6_HPP
int tpc_h_q6_blb_benchmark(int &subsample_number, int &resample_number, float &zf, float &gamma,
                           tpc_h_data_struct &tpc_h_data_struct,
                           std::vector<std::vector<std::string>> &output_data_to_csv,
                           std::vector<std::string> &output_data_
    
    
    
    
    
    
    , char &write_frequency_to_disk,
                           std::uintmax_t &rows_n);

void tpc_h6_blb_prefiltered (sample_struct_lineitem const &coeff_data, sample_struct_lineitem const &query_data, int bootstrap_size);
void tpc_h6_blb_prefiltered_openmp (std::vector<float> & query_data, std::vector<float> & resampling_coeffs,
                                    int subsample_size, int resample_size, int current_subsample,
                                    int current_resample, int resample_num);

void TPC_Q6_pre_query (sample_struct_lineitem &query_data, int table_size);
void multi_threaded_subsamples_replication(std::vector<sample_struct_lineitem>& subsample_data_vec, std::vector<std::vector<std::vector<float>>>& data_for_resampling, int subsample_number, int resample_number);
void multi_threaded_blb_resampling(std::vector<std::vector<std::vector<float>>>& data_for_resampling, std::vector<std::vector<std::vector<float>>>& resampling_coeffs, int subsample_number, int resample_number, int subsample_size, int resample_size);
void blb_aggregation(std::vector<std::vector<std::vector<float>>> data_for_resampling, std::vector<double>& blb_query_sum);

#endif //TPC_H_Q6_HPP
