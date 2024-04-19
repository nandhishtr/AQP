//
// Created by burtsev on 12/9/22.
//
#include "functions.h"
#include "data_tables.h"

#include <cmath>
#include <vector>
#include <string>
#include <fstream>
#include <random>
#include <iostream>
#include <sstream>
#include <set>
#include <filesystem>
#include <map>
#include <iomanip>


#include "..\exp\comprehensive_exp.h"
#include "..\exp\exp_common.h"

 void subsample_rows(const sample_struct_lineitem& data, std::size_t numSamples, sample_struct_lineitem& subsampled_data ) {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<std::size_t> distribution(0, data.l_orderkey.size() - 1);

    //sample_struct_lineitem subsampled_data(numSamples);

    for (std::size_t i = 0; i < numSamples; ++i) {
        std::size_t randomIndex = distribution(generator);

        subsampled_data.l_orderkey.emplace_back(data.l_orderkey[randomIndex]);
        subsampled_data.l_partkey.emplace_back(data.l_partkey[randomIndex]);
        subsampled_data.l_suppkey.emplace_back(data.l_suppkey[randomIndex]);
        subsampled_data.l_linenumber.emplace_back(data.l_linenumber[randomIndex]);
        subsampled_data.l_quantity.emplace_back(data.l_quantity[randomIndex]);
        subsampled_data.l_extendedprice.emplace_back(data.l_extendedprice[randomIndex]);
        subsampled_data.l_discount.emplace_back(data.l_discount[randomIndex]);
        subsampled_data.l_tax.emplace_back(data.l_tax[randomIndex]);
        subsampled_data.l_returnflag.emplace_back(data.l_returnflag[randomIndex]);
        subsampled_data.l_linestatus.emplace_back(data.l_linestatus[randomIndex]);
        subsampled_data.l_shipdate.emplace_back(data.l_shipdate[randomIndex]);
        subsampled_data.l_commidate.emplace_back(data.l_commidate[randomIndex]);
        subsampled_data.l_receiptdate.emplace_back(data.l_receiptdate[randomIndex]);
        subsampled_data.l_shipinstruct.emplace_back(data.l_shipinstruct[randomIndex]);
        subsampled_data.l_shipmode.emplace_back(data.l_shipmode[randomIndex]);
        subsampled_data.l_comment.emplace_back(data.l_comment[randomIndex]);

    }

}

sample_struct_lineitem subsample_rows_w_return(const sample_struct_lineitem& data, std::size_t numSamples ) {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<std::size_t> distribution(0, data.l_orderkey.size() - 1);

    sample_struct_lineitem subsampled_data(numSamples);

    for (std::size_t i = 0; i < numSamples; ++i) {
        std::size_t randomIndex = distribution(generator);

        subsampled_data.l_orderkey.emplace_back(data.l_orderkey[randomIndex]);
        subsampled_data.l_partkey.emplace_back(data.l_partkey[randomIndex]);
        subsampled_data.l_suppkey.emplace_back(data.l_suppkey[randomIndex]);
        subsampled_data.l_linenumber.emplace_back(data.l_linenumber[randomIndex]);
        subsampled_data.l_quantity.emplace_back(data.l_quantity[randomIndex]);
        subsampled_data.l_extendedprice.emplace_back(data.l_extendedprice[randomIndex]);
        subsampled_data.l_discount.emplace_back(data.l_discount[randomIndex]);
        subsampled_data.l_tax.emplace_back(data.l_tax[randomIndex]);
        subsampled_data.l_returnflag.emplace_back(data.l_returnflag[randomIndex]);
        subsampled_data.l_linestatus.emplace_back(data.l_linestatus[randomIndex]);
        subsampled_data.l_shipdate.emplace_back(data.l_shipdate[randomIndex]);
        subsampled_data.l_commidate.emplace_back(data.l_commidate[randomIndex]);
        subsampled_data.l_receiptdate.emplace_back(data.l_receiptdate[randomIndex]);
        subsampled_data.l_shipinstruct.emplace_back(data.l_shipinstruct[randomIndex]);
        subsampled_data.l_shipmode.emplace_back(data.l_shipmode[randomIndex]);
        subsampled_data.l_comment.emplace_back(data.l_comment[randomIndex]);

    }

    return subsampled_data;
}




std::map<float, int> calculate_frequency(const std::vector<float>& values) {
    std::map<float, int> frequencyMap;

    for (float value : values) {
        // If the value is not present in the map, insert it with count 1
        // If it's already present, increment its count
        frequencyMap[value]++;
    }

    return frequencyMap;
}

std::map<int, int> calculate_frequency(const std::vector<int>& values) {
    std::map<int, int> frequencyMap;

    for (int value : values) {
        // If the value is not present in the map, insert it with count 1
        // If it's already present, increment its count
        frequencyMap[value]++;
    }

    return frequencyMap;
}

std::map<char, int> calculate_frequency(const std::vector<char>& values) {
    std::map<char, int> frequencyMap;

    for (char value : values) {
        // If the value is not present in the map, insert it with count 1
        // If it's already present, increment its count
        frequencyMap[value]++;
    }

    return frequencyMap;
}

std::map<std::string, int> calculate_frequency(const std::vector<std::string>& values) {
    std::map<std::string, int> frequencyMap;

    for (const std::string& value : values) {
        // If the value is not present in the map, insert it with count 1
        // If it's already present, increment its count
        frequencyMap[value]++;
    }

    return frequencyMap;
}

void write_frequency_table_to_CSV(const std::map<float, int>& frequencyMap, const std::string& outputFilepath, const std::string& filename) {
    create_directory_if_not_exists(outputFilepath);
    std::ofstream outputFile(outputFilepath + "/" + filename);

    if (outputFile.is_open()) {
        // Write header
        outputFile << "Value,Frequency\n";

        for (const auto& pair : frequencyMap) {
            float value = pair.first;
            int frequency = pair.second;

            // Write value and frequency to the CSV file
            outputFile << value << "," << frequency << "\n";
        }

        outputFile.close();
        std::cout << "Frequency table written to " << filename << std::endl;
    } else {
        std::cerr << "Failed to open file: " << filename << std::endl;
    }
}

void write_frequency_table_to_CSV(const std::map<int, int>& frequencyMap, const std::string& outputFilepath, const std::string& filename) {
    create_directory_if_not_exists(outputFilepath);
    std::ofstream outputFile(outputFilepath + "/" + filename);

    if (outputFile.is_open()) {
        // Write header
        outputFile << "Value,Frequency\n";

        for (const auto& pair : frequencyMap) {
            int value = pair.first;
            int frequency = pair.second;

            // Write value and frequency to the CSV file
            outputFile << value << "," << frequency << "\n";
        }

        outputFile.close();
        std::cout << "Frequency table written to " << filename << std::endl;
    } else {
        std::cerr << "Failed to open file: " << filename << std::endl;
    }
}

void write_frequency_table_to_CSV(const std::map<char, int>& frequencyMap, const std::string& outputFilepath, const std::string& filename) {
    create_directory_if_not_exists(outputFilepath);
    std::ofstream outputFile(outputFilepath + "/" + filename);

    if (outputFile.is_open()) {
        // Write header
        outputFile << "Value,Frequency\n";

        for (const auto& pair : frequencyMap) {
            char value = pair.first;
            int frequency = pair.second;

            // Write value and frequency to the CSV file
            outputFile << value << "," << frequency << "\n";
        }

        outputFile.close();
        std::cout << "Frequency table written to " << filename << std::endl;
    } else {
        std::cerr << "Failed to open file: " << filename << std::endl;
    }
}

bool create_directory_if_not_exists(const std::string& directoryPath) {
    /*if (!std::filesystem::exists(directoryPath)) {
        if (std::filesystem::create_directory(directoryPath)) {
            std::cout << "Directory created: " << directoryPath << std::endl;
            return true;
        }
        else {
            std::cerr << "Failed to create directory: " << directoryPath << std::endl;
            return false;
        }
    }

    std::cout << "Directory already exists: " << directoryPath << std::endl;*/
    return true;
}

void write_frequency_table_to_CSV(const std::map<std::string, int>& frequencyMap, const std::string& outputFilepath, const std::string& filename) {
    create_directory_if_not_exists(outputFilepath);
    std::ofstream outputFile(outputFilepath + "/" + filename);

    if (outputFile.is_open()) {
        // Write header
        outputFile << "Value,Frequency\n";

        for (const auto& pair : frequencyMap) {
            std::string value = pair.first;
            int frequency = pair.second;

            // Write value and frequency to the CSV file
            outputFile << value << "," << frequency << "\n";
        }

        outputFile.close();
        std::cout << "Frequency table written to " << filename << std::endl;
    } else {
        std::cerr << "Failed to open file: " << filename << std::endl;
    }
}


template<typename T>
std::string get_type_name() {
    return typeid(T).name();
}

void write_data_frequencies_to_csv(sample_struct_lineitem const &data, const std::string& zf){


    std::map<int, int> l_orderkeyFrequencyMap = calculate_frequency(data.l_orderkey);
    write_frequency_table_to_CSV(l_orderkeyFrequencyMap, "FT_orderkey", zf + "FT_l_orderkey.csv");

    std::map<int, int> l_partkeyFrequencyMap = calculate_frequency(data.l_partkey);
    write_frequency_table_to_CSV(l_partkeyFrequencyMap, "FT_partkey", zf + "FT_l_partkey.csv");

    std::map<int, int> l_suppkeyFrequencyMap = calculate_frequency(data.l_suppkey);
    write_frequency_table_to_CSV(l_suppkeyFrequencyMap, "FT_suppkey", zf + "FT_l_suppkey.csv");

    std::map<int, int> l_linenumberFrequencyMap = calculate_frequency(data.l_linenumber);
    write_frequency_table_to_CSV(l_linenumberFrequencyMap, "FT_linenumber", zf + "FT_l_linenumber.csv");

    std::map<float, int> l_quantityFrequencyMap = calculate_frequency(data.l_quantity);
    write_frequency_table_to_CSV(l_quantityFrequencyMap, "FT_quantity", zf + "FT_l_quantity.csv");

    std::map<float, int> l_extendedpriceFrequencyMap = calculate_frequency(data.l_extendedprice);
    write_frequency_table_to_CSV(l_extendedpriceFrequencyMap, "FT_extendedprice", zf + "FT_l_extendedprice.csv");

    std::map<float, int> l_discountFrequencyMap = calculate_frequency(data.l_discount);
    write_frequency_table_to_CSV(l_discountFrequencyMap, "FT_discount", zf + "FT_l_discount.csv");

    std::map<float, int> l_taxFrequencyMap = calculate_frequency(data.l_tax);
    write_frequency_table_to_CSV(l_taxFrequencyMap, "FT_tax", zf + "FT_l_tax.csv");

    std::map<char, int> l_returnflagFrequencyMap = calculate_frequency(data.l_returnflag);
    write_frequency_table_to_CSV(l_returnflagFrequencyMap, "FT_returnflag", zf + "FT_l_returnflag.csv");

    std::map<char, int> l_linestatusFrequencyMap = calculate_frequency(data.l_linestatus);
    write_frequency_table_to_CSV(l_linestatusFrequencyMap, "FT_linestatus", zf + "FT_l_linestatus.csv");

    // Calculate frequency and write to CSV for l_shipdate
    std::map<int, int> l_shipdateFrequencyMap = calculate_frequency(data.l_shipdate);
    write_frequency_table_to_CSV(l_shipdateFrequencyMap, "FT_shipdate", zf + "FT_l_shipdate.csv");

    // Calculate frequency and write to CSV for l_commidate
    std::map<int, int> l_commidateFrequencyMap = calculate_frequency(data.l_commidate);
    write_frequency_table_to_CSV(l_commidateFrequencyMap, "FT_commidate", zf + "FT_l_commidate.csv");

    // Calculate frequency and write to CSV for l_receiptdate
    std::map<int, int> l_receiptdateFrequencyMap = calculate_frequency(data.l_receiptdate);
    write_frequency_table_to_CSV(l_receiptdateFrequencyMap, "FT_receiptdate", zf + "FT_l_receiptdate.csv");

    // Calculate frequency and write to CSV for l_shipinstruct
    std::map<std::string, int> l_shipinstructFrequencyMap = calculate_frequency(data.l_shipinstruct);
    write_frequency_table_to_CSV(l_shipinstructFrequencyMap, "FT_shipinstruct", zf + "FT_l_shipinstruct.csv");

    // Calculate frequency and write to CSV for l_shipmode
    std::map<std::string, int> l_shipmodeFrequencyMap = calculate_frequency(data.l_shipmode);
    write_frequency_table_to_CSV(l_shipmodeFrequencyMap, "FT_shipmode", zf + "FT_l_shipmode.csv");

    // Calculate frequency and write to CSV for l_comment
//            std::map<std::string, int> l_commentFrequencyMap = calculate_frequency(data.l_comment);
//            write_frequency_table_to_CSV(l_commentFrequencyMap, "FT_comment", zf + "FT_l_comment.csv");

    // Calculate frequency and write to CSV for answer
    std::map<float, int> answerFrequencyMap = calculate_frequency(data.answer);
    write_frequency_table_to_CSV(answerFrequencyMap, "FT_answer", zf + "FT_answer.csv");
}



/*-- TPC-H Query 6
//select
sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= date '1994-01-01'
and l_shipdate < date '1995-01-01'
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24
*/



// Function to sum the values in the coeff vector
float sum_agg(const std::vector<float>& data) {
    float sum = 0.0f;
    for (float value : data) {
        sum += value;
    }
    return sum;
}

double sum_agg_vec(const std::vector<float>& data) {
    double sum = 0.0f;
    for (float value : data) {
        sum += value;
    }
    return sum;
}

// Function to calculate the average of the values in the coeff vector
float average_agg(const std::vector<float>& data) {
    if (data.empty()) {
        return 0.0f;  // Avoid division by zero
    }
    float sum = sum_agg(data);
    return sum / static_cast<float>(data.size());
}

// Function to count the number of values in the coeff vector
std::size_t count_agg(const std::vector<float>& data) {
    return data.size();
}


void read_vector_from_file(const std::string& filePath, std::vector<int>& vector) {
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

void read_vector_from_file(const std::string& filePath, std::vector<float>& vector) {
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

void read_vector_from_file(const std::string& filePath, std::vector<char>& vector) {
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

void read_vector_from_file(const std::string& filePath, std::vector<std::string>& vector) {
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

// Function to calculate the mean of a vector of values
double calculate_mean(const std::vector<double>& values) {
    double sum = 0.0;
    for (double value : values) {
        sum += value;
    }
    return sum / double(values.size());
}

// Function to calculate the mean of a vector of values
double calculateMean_internal(const std::vector<double>& values) {
    double sum = 0.0;
    for (double value : values) {
        sum += value;
    }
    return sum / double(values.size());
}

// Function to calculate the standard deviation of a vector of values
double calculateStandardDeviation(const std::vector<double>& values) {
    double mean = calculateMean_internal(values);
    double sumSquaredDiff = 0.0;
    for (double value : values) {
        double diff = value - mean;
        sumSquaredDiff += diff * diff;
    }
    return std::sqrt(sumSquaredDiff / double(values.size() - 1));
}

// Function to calculate the standard error of a vector of values
double calculate_standard_error(const std::vector<double>& values) {
    double standardDeviation = calculateStandardDeviation(values);
    return standardDeviation / std::sqrt(values.size());
}

// Function to calculate the confidence interval of a vector of values
std::pair<double, double> calculate_confidence_interval(const std::vector<double>& values, double confidenceLevel) {
    double lowerPercentile = double(1 - confidenceLevel) / 2 * 100;
    double upperPercentile = double(1 + confidenceLevel) / 2 * 100;

    std::vector<double> sortedValues = values;
    std::sort(sortedValues.begin(), sortedValues.end());

    int lowerIndex = int(lowerPercentile * sortedValues.size()) / 100;
    int upperIndex = int(upperPercentile * sortedValues.size()) / 100;

    return std::make_pair(sortedValues[lowerIndex], sortedValues[upperIndex]);
}

void remove_zeros_from_vec(std::vector<float>& vec) {
    vec.erase(std::remove(vec.begin(), vec.end(), 0), vec.end());
}


std::vector<std::string> round_to_one_decimal_and_convert_to_string(const std::vector<float>& values) {
    std::vector<std::string> roundedStrings;
    for (float value : values) {
        // Round the float value to two decimal places
        float roundedValue = std::round(value * 10) / 10;

        // Convert the rounded value to a string with two digits after the decimal point
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1) << roundedValue;
        roundedStrings.push_back(ss.str());
    }
    return roundedStrings;
}



std::vector<std::vector<std::string>> output_file_rows_names_write (){
    std::vector<std::string> output_data_column;
    std::vector<std::vector<std::string>> output_data_to_csv;
    output_data_column.emplace_back("Skew factor Z: ");
    output_data_column.emplace_back("Preparation took in ms: ");
//    output_data_column.emplace_back("Cache reading took: ");
    output_data_column.emplace_back("Subsampling took: ");
    output_data_column.emplace_back("Filtering took: ");
    output_data_column.emplace_back("Data preparation for resampling took: ");
    output_data_column.emplace_back("Resampling took: ");
    output_data_column.emplace_back("Aggregation took: ");
    output_data_column.emplace_back("Full query execution: ");
    output_data_column.emplace_back("Full q6 answer : ");
    output_data_column.emplace_back("BLB query answer: ");
    output_data_column.emplace_back("BLB query answer standart error: ");
    output_data_column.emplace_back("BLB query answer confidence Interval min: ");
    output_data_column.emplace_back("BLB query answer confidence Interval max: ");
//    output_data_column.emplace_back("Overall execution : ");
    output_data_column.emplace_back("Overall execution without data read: ");
    output_data_to_csv.emplace_back(output_data_column);
    output_data_column.clear();
    return output_data_to_csv;
}
void show_available_commands() {
    std::cout << "Available commands:" << std::endl;
    std::cout << " - q - stop program" << std::endl;
    std::cout << " - sf - change scale factor" << std::endl;
    std::cout << " - zf - change skew factor" << std::endl;
    std::cout << " - g - change BLB gamma value"  << std::endl;
    std::cout << " - sn - change BLB subsamples amount" << std::endl;
    std::cout << " - rn - change BLB resampler instances amount" << std::endl;
    std::cout << " - sv - show variables for benchmark run" << std::endl;
    std::cout << " - b - run benchmark with current parameters" << std::endl;
    // Add descriptions for more commands if needed
}



void write_2d_vector_to_csv(const std::vector<std::vector<std::string>>& data, const std::string& filename) {
    std::ofstream outputFile(filename);

    if (outputFile.is_open()) {
        // Get the number of rows and columns
        size_t numRows = data.size() - 1 ;
        size_t numCols = data[0].size();

        // Write the data column-wise
        for (size_t col = 0; col < numCols; ++col) {
            for (size_t row = 0; row < numRows; ++row) {
                outputFile << data[row][col] << ",";
            }
            outputFile << "\n";
        }

        outputFile.close();
        std::cout << "Data written to " << filename << std::endl;
    } else {
        std::cerr << "Failed to open file: " << filename << std::endl;
    }
}

void aqpp_blb_filtering(std::vector <std::vector<double>> subsample, std::vector<aqppp::Condition> cur_q, sample_struct_lineitem& query_data )
{

    //#pragma omp parallel for shared(filtered_data) schedule(static, table_size/20)
    auto t0 = std::chrono::high_resolution_clock::now();
    //#pragma omp parallel for shared(filtered_data) schedule(static, table_size/k )
    for (int i = 0; i < subsample[0].size(); i++) {
        if (subsample[1][i] >= cur_q[0].lb && subsample[1][i] < cur_q[0].ub ||
            subsample[2][i] >= cur_q[1].lb && subsample[2][i] < cur_q[1].ub)
                query_data.answer.emplace_back(subsample[0][i]);
    }
    //auto t1 = std::chrono::high_resolution_clock::now();
    //#pragma omp parallel for shared(filtered_data) schedule(static, table_size/k )
    float product = 0;
    /*for (int i = 0; i < sample[0].size(); i++) {
        query_data.answer.push_back(0);
        if (query_data.id[i])
            product = query_data.l_extendedprice[i] * (1 - query_data.l_discount[i]);
        query_data.answer.push_back(product);
    }*/
}


void multi_threaded_filtering(std::vector<sample_struct_lineitem>& BLB_subsample_data_vec, std::vector <std::vector <std::vector<double>>>& aqpp_blb_sample, std::vector<aqppp::Condition>& cur_q) {
    std::vector<std::thread> threads;

    threads.reserve(aqpp_blb_sample.size());
    for (int i = 0; i < aqpp_blb_sample.size(); ++i) {
        threads.emplace_back([&BLB_subsample_data_vec, i, &aqpp_blb_sample, cur_q]() {
            aqpp_blb_filtering(aqpp_blb_sample[i], cur_q, BLB_subsample_data_vec[i]);
            });
    }

    // Wait for all threads to finish
    for (std::thread& thread : threads) {
        thread.join();
    }
    threads.clear();
}
