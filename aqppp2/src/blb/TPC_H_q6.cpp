//
// Created by hti on 10/6/23.
//

#include "TPC_H_q6.hpp"
#include "functions.h"
#include "data_tables.h"
#include <cmath>
#include <vector>
#include <string>
#include <chrono>
#include <iostream>
#include <set>
#include <thread>
//#include <gsl/gsl_rng.h>
//#include <gsl/gsl_randist.h>

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::duration;
using std::chrono::milliseconds;
using std::chrono::microseconds;



void tpc_h6_blb_prefiltered (sample_struct_lineitem const &coeff_data, sample_struct_lineitem const &query_data, int bootstrap_size)
{
    int success_try;
    float revenue = 0.0;
    success_try = 0;
    for (int i = 0; i < bootstrap_size; i++)
    {

        success_try +=1;
        //std::cout << revenue <<   std::endl;
        revenue += (float)query_data.l_extendedprice[i] *  (1-query_data.l_discount[i]) * coeff_data.coeff[i];
        //std::cout << "id "<<  query_data.id[i] <<  " price "<<  query_data.l_extendedprice[i] << " discount "<<  query_data.l_discount[i] << " coeff "<< coeff_data.coeff[i] <<   std::endl;
        //std::cout << "within cycle "<<revenue << std::endl;
    }
    //std::cout << "after cycle "<<revenue << " success "<< success_try << " j "<< j <<   std::endl;

}


void tpc_h6_blb_prefiltered_openmp (std::vector<float> & query_data, std::vector<float> & resampling_coeffs, int subsample_size, int resample_size, int current_subsample, int current_resample, int resample_num)
{
    //float resample_size = std::pow(subsample_size, 1/gamma);//599860520
    float GRN_zig;
    float GRN;
    //printf ("rnd_val: %d\n", int(rnd));

    int success_try;
    std::random_device rd{};
    //GSL gen--------------
//    const gsl_rng_type * T;
//    gsl_rng * r;
//    gsl_rng_env_setup();
//    T = gsl_rng_default;
//    gsl_rng_default_seed =  current_resample + current_subsample * resample_num  ;

//    r = gsl_rng_alloc (T);
// Set the seed for the GSL random number generator
    //gsl_rng_set(r, gsl_rng_default_seed);
    //--------------------
    //std::mt19937 gen{rd()};
    std::minstd_rand gen(rd());
    //std::subtract_with_carry_engine<unsigned, 24,10,24> gen(rd());
    //XoshiroCpp::Xoshiro256PlusPlus gen(rd());
    float resampling_coeff_new_mean = float(resample_size)/float(subsample_size);
    std::normal_distribution<> d{(double)resampling_coeff_new_mean,sqrt(resample_size/subsample_size)};
    //double gsl_ran_gaussian;
    //success_try = 0;
    //#pragma omp parallel for reduction(+:revenue)
    //--STD lib resampling-----

    for (size_t i = 0; i < query_data.size(); ++i) {
        float random_num = d(gen);
        query_data[i] *= random_num;
        resampling_coeffs.push_back(GRN);

    }
//    for (float & i : query_data)
//    {
        //success_try +=1;
        //--STD lib resampling-----
//        GRN =  d(gen);
//        query_data[i] = query_data[i] * GRN;
//        i = i * GRN ;
//        resampling_coeffs.push_back(GRN);
        //-------------------------
        //--GSL lib resampling-----------
//        GRN_zig = float(gsl_ran_gaussian_ziggurat(r,std::sqrt(resampling_coeff_new_mean)) + resampling_coeff_new_mean);
//        i = i * GRN_zig ;
//        resampling_coeffs.push_back(GRN_zig);
        //--------------------------------
//    }
//    gsl_rng_free (r);
}


void TPC_Q6_pre_query (sample_struct_lineitem &query_data, int table_size)
{

    //#pragma omp parallel for shared(filtered_data) schedule(static, table_size/20)
    auto t0 = std::chrono::high_resolution_clock::now();
    //#pragma omp parallel for shared(filtered_data) schedule(static, table_size/k )
    for(int i=0;i < table_size;i++){
        query_data.id.push_back(0);
        if (query_data.l_shipdate[i] >= 19940101 && query_data.l_shipdate[i] < 19950101 &&
            query_data.l_discount[i] > 0.06 - 0.01 && query_data.l_discount[i] < 0.06 + 0.01 &&
            query_data.l_quantity[i] < 24)
            query_data.id[i] = 1;

    }
    //auto t1 = std::chrono::high_resolution_clock::now();
    //#pragma omp parallel for shared(filtered_data) schedule(static, table_size/k )
    float product = 0;
    for(int i=0;i < table_size;i++){
        query_data.answer.push_back(0);
        if (query_data.id[i])
            product =  query_data.l_extendedprice[i] * (1-query_data.l_discount[i]) ;
        query_data.answer.push_back(product);
    }
}


void single_threaded_subsampling(const sample_struct_lineitem& data_lineitem, int subsample_number, int subsample_size, std::vector<sample_struct_lineitem>& subsample_data_vec) {
    subsample_data_vec.reserve(subsample_number);

    for (int i = 0; i < subsample_number; ++i) {
        sample_struct_lineitem subsample = subsample_rows_w_return(data_lineitem, subsample_size);
        subsample_data_vec.push_back(subsample);
    }
}

void multi_threaded_subsampling(const sample_struct_lineitem& data_lineitem, int subsample_number, int subsample_size, std::vector<sample_struct_lineitem>& subsample_data_vec) {
    sample_struct_lineitem single_subsample(subsample_size);
    subsample_data_vec.assign(subsample_number, single_subsample);

    std::vector<std::thread> threads;

    threads.reserve(subsample_number);
for (int i = 0; i < subsample_number; ++i) {
        threads.emplace_back([&, i] {
            subsample_rows(data_lineitem, subsample_size, subsample_data_vec[i]);
        });
    }

    // Wait for all threads to finish
    for (std::thread& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void single_threaded_TPC_Q6_pre_query(std::vector<sample_struct_lineitem>& subsample_data_vec, int subsample_number, int subsample_size) {
    for (int i = 0; i < subsample_number; ++i) {
        TPC_Q6_pre_query(subsample_data_vec[i], subsample_size);
    }

}
void multi_threaded_TPC_Q6_pre_query(std::vector<sample_struct_lineitem>& subsample_data_vec, int subsample_number, int subsample_size) {
    std::vector<std::thread> threads;

    threads.reserve(subsample_number);
for (int i = 0; i < subsample_number; ++i) {
        threads.emplace_back([&subsample_data_vec, i, subsample_size]() {
            TPC_Q6_pre_query(subsample_data_vec[i], subsample_size);
        });
    }

    // Wait for all threads to finish
    for (std::thread& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void single_threaded_subsamples_replication(std::vector<sample_struct_lineitem>& subsample_data_vec, std::vector<std::vector<std::vector<float>>>& data_for_resampling, int subsample_number, int resample_number) {
    for (int subsample = 0; subsample < subsample_number; ++subsample) {
        remove_zeros_from_vec(subsample_data_vec[subsample].answer);
        for (int resample_iter = 0; resample_iter < resample_number; ++resample_iter) {
            data_for_resampling[subsample][resample_iter] = subsample_data_vec[subsample].answer;
            remove_zeros_from_vec(data_for_resampling[subsample][resample_iter]);
        }
    }
}
void multi_threaded_subsamples_replication(std::vector<sample_struct_lineitem>& subsample_data_vec, std::vector<std::vector<std::vector<float>>>& data_for_resampling, int subsample_number, int resample_number) {
    std::vector<std::thread> threads;

    threads.reserve(subsample_number);
for (int subsample = 0; subsample < subsample_number; ++subsample) {
        threads.emplace_back([&subsample_data_vec, &data_for_resampling, subsample, resample_number]() {
            remove_zeros_from_vec(subsample_data_vec[subsample].answer);

            // Capture resample_number in the lambda capture list
            for (int resample_iter = 0; resample_iter < resample_number; ++resample_iter) {
                data_for_resampling[subsample][resample_iter] = subsample_data_vec[subsample].answer;
                remove_zeros_from_vec(data_for_resampling[subsample][resample_iter]);
            }
        });
    }

    subsample_data_vec.clear();

    // Wait for all threads to finish
    for (std::thread& thread : threads) {
        thread.join();
    }
    threads.clear();
}
void single_threaded_blb_resampling(std::vector<std::vector<std::vector<float>>>& data_for_resampling, std::vector<std::vector<std::vector<float>>>& resampling_coeffs, int subsample_number, int resample_number, int subsample_size, int resample_size) {

    for (int i = 0; i < subsample_number; i++) {
        for (int j = 0; j < resample_number; j++) {
            tpc_h6_blb_prefiltered_openmp(data_for_resampling[i][j], resampling_coeffs[i][j], subsample_size,
                                          resample_size, i, j, resample_number);
        }
    }
}
void multi_threaded_blb_resampling(std::vector<std::vector<std::vector<float>>>& data_for_resampling, std::vector<std::vector<std::vector<float>>>& resampling_coeffs, int subsample_number, int resample_number, int subsample_size, int resample_size) {
    std::vector<std::thread> threads;

    for (int i = 0; i < subsample_number; i++) {
        for (int j = 0; j < resample_number; j++) {
            threads.emplace_back([&data_for_resampling, &resampling_coeffs, subsample_size, resample_size, i, j, resample_number]() {
                tpc_h6_blb_prefiltered_openmp(data_for_resampling[i][j], resampling_coeffs[i][j], subsample_size, resample_size, i, j, resample_number);
            });
        }
    }

    // Wait for all threads to finish
    for (std::thread& thread : threads) {
        thread.join();
    }
    threads.clear();
}

void blb_aggregation(std::vector<std::vector<std::vector<float>>> data_for_resampling, std::vector<double>& blb_query_sum) {
    blb_query_sum.reserve(data_for_resampling.size() * data_for_resampling[0].size());

    for (const auto& outer_vector : data_for_resampling) {
        for (const auto& inner_vector : outer_vector) {
            // Call the function with the innerVector
            blb_query_sum.push_back(sum_agg_vec(inner_vector));
        }
    }
    data_for_resampling.clear();
}

void process_data_lineitem(sample_struct_lineitem& data_lineitem, std::vector<double>& blb_query_sum, char write_frequency_to_disk, float zf) {
    remove_zeros_from_vec(data_lineitem.answer);
    blb_query_sum.push_back(sum_agg_vec(data_lineitem.answer));

    if (write_frequency_to_disk == 'y') {
        // Calculate the frequency of values in the vector and write to folder columnwise
        write_data_frequencies_to_csv(data_lineitem, std::to_string(zf));
    }
}


int tpc_h_q6_blb_benchmark(int &subsample_number, int &resample_number, float &zf, float &gamma,
                           tpc_h_data_struct &tpc_h_data_struct,
                           std::vector<std::vector<std::string>> &output_data_to_csv,
                           std::vector<std::string> &output_data_column, char &write_frequency_to_disk,
                           std::uintmax_t &rows_n) {
    int benchmark_done;
    sample_struct_lineitem data_lineitem(0);


    //std::vector<std::string> rounded_zf_string = round_to_one_decimal_and_convert_to_string(zf_vector);

    //for (float BLB_gamma : gamma_vector) {

    auto t0 = high_resolution_clock::now();
    data_lineitem = tpc_h_data_struct.lineitem_table;
    // Set read_new_data to 'n' (assuming it's a flag)
    rows_n = data_lineitem.l_orderkey.size();

    auto t1 = high_resolution_clock::now();
//    auto t2 = high_resolution_clock::now();
//    if (read_new_data =='y'){
//
//
//        t1 = high_resolution_clock::now();
//        read_new_data = 'n';
//     t2 = high_resolution_clock::now();
//    }

    int subsample_size = int(pow(int(rows_n), double(gamma)));
    int resample_size = int(rows_n);
    std::vector<sample_struct_lineitem> subsample_data_vec;
    subsample_data_vec.reserve(subsample_number);
    sample_struct_lineitem single_subsample(subsample_size);


    std::vector<std::thread> threads;
    //--------------single thread subsampling
    //single_threaded_subsampling(data_lineitem, subsample_number, subsample_size, subsample_data_vec);


    //---------------------multithread subsampling
    multi_threaded_subsampling(data_lineitem, subsample_number, subsample_size, subsample_data_vec);

    auto t3 = high_resolution_clock::now();
    //-----------single thread filtering-----------
    //single_threaded_TPC_Q6_pre_query(subsample_data_vec, subsample_number, subsample_size);

    //-----------multithread filtering-----------
    multi_threaded_TPC_Q6_pre_query(subsample_data_vec, subsample_number, subsample_size);

    auto t4 = high_resolution_clock::now();

    std::string value;

    std::vector<std::vector<std::vector<float>>> data_for_resampling(subsample_number,
                                                                     std::vector<std::vector<float>>(
                                                                             resample_number,
                                                                             std::vector<float>(0)));
    // --------- single thread populating data_for_resampling

    //single_threaded_subsamples_replication(subsample_data_vec, data_for_resampling, subsample_number, resample_number);

    // --------- multithread populating data_for_resampling
    multi_threaded_subsamples_replication(subsample_data_vec, data_for_resampling, subsample_number, resample_number);
    auto t5 = high_resolution_clock::now();
    //---------------------------------------------

    std::vector<std::vector<std::vector<float>>> resampling_coeffs(subsample_number,
                                                                   std::vector<std::vector<float>>(
                                                                           resample_number,
                                                                           std::vector<float>(0)));

    //----------single thread resampling
    //single_threaded_blb_resampling(data_for_resampling, resampling_coeffs, subsample_number, resample_number, subsample_size, resample_size);

    //----------multithread resampling
    multi_threaded_blb_resampling(data_for_resampling, resampling_coeffs, subsample_number, resample_number, subsample_size, resample_size);
    auto t6 = high_resolution_clock::now();
    //-----------------------------------------

    //----------BLB Aggregation

    std::vector<double> blb_query_sum;
    blb_aggregation(data_for_resampling, blb_query_sum);


    double blb_query_answer = calculate_mean(blb_query_sum);
    double blb_query_answer_standart_error = calculate_standard_error(blb_query_sum);
    std::pair<double, double> conf_interval = calculate_confidence_interval(blb_query_sum, 0.95);
    auto t7 = high_resolution_clock::now();

    // Fill the answer vector with zeros using assign()
    data_lineitem.answer.assign(data_lineitem.answer.size(), 0);
    // Calculation of query answer for full data set
    TPC_Q6_pre_query(data_lineitem, int(rows_n));
    double full_query_sum = sum_agg_vec(data_lineitem.answer);

    //Calculation frequencies for answer values for full dataset
    process_data_lineitem(data_lineitem, blb_query_sum, write_frequency_to_disk, zf);
//    remove_zeros_from_vec(data_lineitem.answer);
//    blb_query_sum.push_back(sum_agg_vec(data_lineitem.answer));
//    if (write_frequency_to_disk == 'y'){
//        // Calculate the frequency of values in the vector and write to folder columnwise
//        write_data_frequencies_to_csv(data_lineitem, std::to_string(zf));
//    }




    auto t8 = high_resolution_clock::now();

    auto preparation_time = duration_cast<microseconds>(t1 - t0);
//    auto cache_reading_time = duration_cast<microseconds>(t2 - t1);
    auto subsampling_time = duration_cast<microseconds>(t3 - t1);
    auto filtering_time = duration_cast<microseconds>(t4 - t3);
    auto data_preparation_for_resampling_time = duration_cast<microseconds>(t5 - t4);
    auto resampling_time = duration_cast<microseconds>(t6 - t5);
    auto aggregation_time = duration_cast<microseconds>(t7 - t6);
    //auto overall_execution = duration_cast<microseconds>(t7 - t0);
    auto overall_execution_no_cache_read = duration_cast<microseconds>(t7 - t1);
    auto full_query_execution = duration_cast<microseconds>(t8 - t7);


    std::cout << std::endl;
    std::cout << "Preparation took: " << float(preparation_time.count()) / 1000 << " ms " << std::endl;
//    std::cout << "Cache reading took: " << float(cache_reading_time.count()) / 1000 << " ms " << std::endl;
    std::cout << "Subsampling took: " << float(subsampling_time.count()) / 1000 << " ms " << std::endl;
    std::cout << "Filtering took: " << float(filtering_time.count()) / 1000 << " ms " << std::endl;
    std::cout << "Data preparation for resampling took: "
              << float(data_preparation_for_resampling_time.count()) / 1000 << " ms " << std::endl;
    std::cout << "Resampling took: " << float(resampling_time.count()) / 1000 << " ms " << std::endl;
    std::cout << "Aggregation took: " << float(aggregation_time.count()) / 1000 << " ms " << std::endl;
    std::cout << std::endl;
    std::cout << "Size of cached data_lineitem table: " << data_lineitem.l_orderkey.size() << std::endl;
    std::cout << "Full dataset query execution: " <<  float(full_query_execution.count()) / 1000 << " ms "
              << std::endl;
    std::cout << "Full q6 answer: " << full_query_sum << std::endl;
    std::cout << std::endl;
    std::cout << "Subsample size: " << subsample_size << std::endl;
    std::cout << "BLB query answer: " << blb_query_answer << std::endl;
    std::cout << "BLB query answer standart error: " << blb_query_answer_standart_error << std::endl;
    std::cout << "BLB query answer: Confidence Interval: [" << conf_interval.first << ", "
              << conf_interval.second << "]" << std::endl;
    //std::cout << "Overall execution : " << float(overall_execution.count()) / 1000 << " ms " << std::endl;
    std::cout << "Overall execution without data_lineitem read: "
              << float(overall_execution_no_cache_read.count()) / 1000
              << " ms " << std::endl;
    output_data_column.emplace_back(std::to_string(zf));
    output_data_column.emplace_back(std::to_string(float(preparation_time.count()) / 1000));
//    output_data_column.emplace_back(std::to_string(float(cache_reading_time.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(subsampling_time.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(filtering_time.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(data_preparation_for_resampling_time.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(resampling_time.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(aggregation_time.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(full_query_execution.count()) / 1000));
    output_data_column.emplace_back(std::to_string(full_query_sum));
    output_data_column.emplace_back(std::to_string(blb_query_answer));
    output_data_column.emplace_back(std::to_string(blb_query_answer_standart_error));
    output_data_column.emplace_back(std::to_string(conf_interval.first));
    output_data_column.emplace_back(std::to_string(conf_interval.second));
    //output_data_column.emplace_back(std::to_string(float(overall_execution.count()) / 1000));
    output_data_column.emplace_back(std::to_string(float(overall_execution_no_cache_read.count()) / 1000));

    output_data_to_csv.emplace_back(output_data_column);
    output_data_column.clear();



    output_data_to_csv.emplace_back(output_data_column);
    std::string outfile_name;
    outfile_name = "test_results"+ std::to_string(gamma) +".csv";

    write_2d_vector_to_csv(output_data_to_csv, outfile_name);
    output_data_to_csv.clear();
    benchmark_done = 1;
//    if (errno != 0) {
//        char buffer[256];
////            strerror_r(errno, buffer, 256); // get string message from errno, XSI-compliant version
////            printf("Error %s", buffer);
//        // or
//        char *errorMsg = strerror_r(errno, buffer, 256); // GNU-specific version, Linux default
//        printf("Error %s", errorMsg); //return value has to be used since buffer might not be modified
//    }

    return benchmark_done*errno+1;
}
