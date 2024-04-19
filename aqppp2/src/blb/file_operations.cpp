   //
//class FileReader {
//public:
//    static void read_vector_from_file(const std::string& filePath, std::vector<int>& vector) {
//        std::ifstream file(filePath, std::ios::binary);
//        if (file) {
//            int value;
//            while (file.read(reinterpret_cast<char*>(&value), sizeof(int))) {
//#pragma omp critical
//                vector.push_back(value);
//            }
//            file.close();
//        } else {
//            std::cerr << "Failed to open file: " << filePath << std::endl;
//        }
//    }
//
//    static void read_vector_from_file(const std::string& filePath, std::vector<float>& vector) {
//        std::ifstream file(filePath, std::ios::binary);
//        if (file) {
//            float value;
//            while (file.read(reinterpret_cast<char*>(&value), sizeof(float))) {
//#pragma omp critical
//                vector.push_back(value);
//            }
//            file.close();
//        } else {
//            std::cerr << "Failed to open file: " << filePath << std::endl;
//        }
//    }
//
//    static void read_vector_from_file(const std::string& filePath, std::vector<char>& vector) {
//        std::ifstream file(filePath);
//        if (file) {
//            std::string line;
//            if (std::getline(file, line)) {
//                for (char c : line) {
//#pragma omp critical
//                    vector.push_back(c);
//                }
//            }
//            file.close();
//        } else {
//            std::cerr << "Failed to open file: " << filePath << std::endl;
//        }
//    }
//
//    static void read_vector_from_file(const std::string& filePath, std::vector<std::string>& vector) {
//        std::ifstream file(filePath);
//        if (file) {
//            std::string value;
//            while (std::getline(file, value)) {
//#pragma omp critical
//                vector.push_back(value);
//            }
//            file.close();
//        } else {
//            std::cerr << "Failed to open file: " << filePath << std::endl;
//        }
//    }
//};
//
//void read_lineitem_to_sample_struct(const std::string& cache_path, sample_struct_lineitem& data) {
//    FileReader fileReader;
//
//    // Read the files and fill the vectors in the `data` structure
//#pragma omp parallel
//    {
//#pragma omp sections nowait
//        {
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_orderkey.bin", data.l_orderkey);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_partkey.bin", data.l_partkey);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_suppkey.bin", data.l_suppkey);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_linenumber.bin", data.l_linenumber);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_quantity.bin", data.l_quantity);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_extendedprice.bin", data.l_extendedprice);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_discount.bin", data.l_discount);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_tax.bin", data.l_tax);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_returnflag.bin", data.l_returnflag);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_linestatus.bin", data.l_linestatus);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_shipdate.bin", data.l_shipdate);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_commidate.bin", data.l_commidate);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_receiptdate.bin", data.l_receiptdate);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_shipinstruct.txt", data.l_shipinstruct);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_shipmode.txt", data.l_shipmode);
//
//#pragma omp section
//            fileReader.read_vector_from_file(cache_path + "/l_comment.txt", data.l_comment);
//        }
//    }
//}