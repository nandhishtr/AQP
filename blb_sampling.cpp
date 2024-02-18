#include"tool.h"
#include"sql_interface.h"
#include<iostream>
#include "exp_common.h"

namespace blb_sampling {



	std::pair<double, double> Read_BLB_Samples(SQLHANDLE& sqlconnectionhandle, aqppp::Settings& o_PAR, int run_num, std::vector<std::vector<double>>& o_sample, std::vector<std::vector<double>>& o_small_sample)
	{
		//o_sample = std::vector<std::vector<double>>();
		//o_small_sample = std::vector<std::vector<double>>();
		std::vector<double> read_sample_times = std::vector<double>();
		std::vector<double> read_small_sample_times = std::vector<double>();
		for (int i = 0; i < run_num; i++)
		{
			double cur_sample_time = aqppp::SqlInterface::ReadDb(sqlconnectionhandle, o_sample, o_PAR.DB_NAME, o_PAR.SAMPLE_NAME, o_PAR.AGGREGATE_NAME, o_PAR.CONDITION_NAMES);
			double cur_small_smaple_time = aqppp::SqlInterface::ReadDb(sqlconnectionhandle, o_small_sample, o_PAR.DB_NAME, o_PAR.SUB_SAMPLE_NAME, o_PAR.AGGREGATE_NAME, o_PAR.CONDITION_NAMES);
			read_sample_times.push_back(cur_sample_time);
			read_small_sample_times.push_back(cur_small_smaple_time);
		}
		double time_read_sample = aqppp::Tool::get_percentile(read_sample_times, 0.5);
		double time_read_small_sample = aqppp::Tool::get_percentile(read_small_sample_times, 0.5);
		o_PAR.SAMPLE_ROW_NUM = o_sample[0].size();
		std::cout << "o_sample" << o_sample[0].size();
		return{ time_read_sample,time_read_small_sample };
	}
	}