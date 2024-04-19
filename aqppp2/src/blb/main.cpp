#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include "data_tables.h"
#include "functions.h"
#include "TPC_H_q6.hpp"

int main() {
    int subsample_number = 10;
    int resample_number = 20;
    int sf = 1;
    float zf = 0.5;
    float gamma = 0.6;
    std::vector<float> zf_vector = {2.0};

    std::vector<float> gamma_vector = {0.6};
    tpc_h_data_struct tpc_h_data_struct(0);
    sample_struct_lineitem data(0);
    std::uintmax_t number_of_rows_in_data;
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    using std::chrono::microseconds;

    std::random_device rd{};
    std::minstd_rand gen(rd());
    float test;
    float resampling_coeff_new_mean = float(10000)/float(10);
    std::normal_distribution<> d{(double)resampling_coeff_new_mean,sqrt(100)};
    test = test *  d(gen);

    //-----New skewed dbgen------
    //const std::string tablepath = "../../../SkewedDataGenerator/sf";
    //-----Old skewed dbgen------
    //const std::string tablepath = "../../../TPC-H-Skew/generated_files/sf";
    //-----No skew dbgen------
    //const std::string tablepath = "../../../TPC-H_V3.0.1/dbgen/generated_files/sf";
    //-----Windows-based dbgen------
    //const std::string tablepath = "../../tpch-new_win/TPCDSkew/generated_files/sf";
    //-----Windows path -----------
    const std::string tablepath = "..\\tpch-new_win\\generated_files\\sf";
    //const std::string filename = "../../../SkewedDataGenerator/sf1/lineitem.tbl";

    std::vector<std::vector<std::string>> output_data_to_csv;
    std::vector<std::string> output_data_column;
    output_data_to_csv = output_file_rows_names_write();
    std::vector<std::string> rounded_zf_string = round_to_one_decimal_and_convert_to_string(zf_vector);

    bool keepRunning = true;
    char read_new_data = 'y';
    //char make_subsampling = 'y';
    char write_frequency_to_disk = 'n';

    //-----------------------------------------------------------------
    char start_exec = 'y';
    int benchmark_done;
    std::cout << std::endl;


    while (keepRunning) {
        std::cout << "Enter a command (type 'h' for available commands): ";
        std::string command;
        std::cin >> command;

        if (command == "q") {
            keepRunning = false;
        } else if (command == "h") {
            show_available_commands();
        } else if (command == "sf") {
            data.clearData();
            read_new_data = 'y';
            std::cout << "Enter new scale factor <int>:";
            std::cin >> sf;
        } else if (command == "zf") {
            data.clearData();
            read_new_data = 'y';

            std::cout << "Enter new skew factor <float>: ";
            float new_zf;
            std::cin >> new_zf;
            zf = new_zf;
        } else if (command == "g") {
            std::cout << "Enter new BLB gamma value: " << std::endl;
            std::cout << "Best are from 0.5 to 0.8 " << std::endl;
            std::cout << "Enter new gamma value <float> : 0.";
            float new_gamma;
            std::cin >> new_gamma;
            gamma = (new_gamma / 10);
        } else if (command == "sn") {
            std::cout << "Enter new amount of subsamples <int>: ";
            std::cin >> subsample_number;
        } else if (command == "rn") {
            std::cout << "Enter new amount of resamples value <int>: ";
            std::cin >> resample_number;
        } else if (command == "wf") {
            std::cout << "Do you want to write values frequencies in every column? y/n ";
            std::cin >> write_frequency_to_disk;
        } else if (command == "sv") {  // Show variables
            std::cout << "data scale factor: " << sf << std::endl;
            std::cout << "data skew factor: " << zf << std::endl;
            std::cout << "BLB gamma factor: " << gamma << std::endl;

            std::cout << "subsample_number: " << subsample_number << std::endl;
            std::cout << "resample_number: " << resample_number << std::endl;
            std::cout << "write values frequencies in every column: " << write_frequency_to_disk << std::endl;

            std::cout << std::endl;
        } else if (command == "b") {
            std::cout << std::endl;

            std::cout << "data scale factor: " << sf << std::endl;
            std::cout << "data skew factor: " << zf << std::endl;
            std::cout << "BLB gamma factor: " << gamma << std::endl;
            std::cout << std::endl;

            std::cout << "subsample_number: " << subsample_number << std::endl;
            std::cout << "resample_number: " << resample_number << std::endl;
            std::cout << std::endl;


            std::cout << "write values frequencies in every column: " << write_frequency_to_disk << std::endl;
            std::cout << std::endl;

            std::cout << "Starting benchmark with following parameters? y/n: " << std::endl;

            std::cin >> start_exec;


            if (start_exec == 'y') {

                auto t0r = high_resolution_clock::now();
                if (read_new_data == 'y') {
                    data_read_from_storage(tablepath, sf, zf, tpc_h_data_struct);
                }
                auto t1r = high_resolution_clock::now();
                auto data_read_and_preparation_time = duration_cast<microseconds>(t1r - t0r);
                std::cout << "Data read and reparation took: " << float(data_read_and_preparation_time.count()) / 1000
                          << " ms " << std::endl;

                benchmark_done = tpc_h_q6_blb_benchmark(subsample_number, resample_number, zf,
                                                        gamma, tpc_h_data_struct,
                                                        output_data_to_csv, output_data_column,
                                                        write_frequency_to_disk,
                                                        number_of_rows_in_data);
                if (benchmark_done == 1) {
                    start_exec = 'b';
                    read_new_data = 'n';
                    command = 'h';
                    benchmark_done = 0;
                    continue;
                }
            } else if (start_exec == 'n') {
                command = "h";
            }

        }

        // Add more commands here as needed

        if (errno != 0) {
            char buffer[256];
//            strerror_s(errno, buffer, 256); // get string message from errno, XSI-compliant version
//            printf("Error %s", buffer);
            // or
//            char *errorMsg = strerror_r(errno, buffer, 256); // GNU-specific version, Linux default
//            printf("Error %s", errorMsg); //return value has to be used since buffer might not be modified
        }
    }

    return 0;
}